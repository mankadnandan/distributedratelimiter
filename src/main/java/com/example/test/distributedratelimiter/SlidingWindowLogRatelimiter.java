package com.example.test.distributedratelimiter;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class SlidingWindowLogRatelimiter {

    private Jedis jedis;

    /*
     * This is a field that is used to store the number of requests that a user can call in a given window of time.
     * It goes into the user's metadata map.
     */
    private static final String REQUESTS = "requests";

    /*
     * This is a field that is used to store the window of time in seconds for which the requests are allowed.
     * It goes into the user's metadata map.
     */
    private static final String WINDOW_TIME = "window_time";

    /*
     * This is a suffix that is appended to the user id to store the user's metadata.
     * It is used to store the number of requests that a user can call in a given window of time.
     * The stucture looks something as follows:
     * {
     *      user1_metadata : {
     *                          requests: 100, 
     *                          window_time: 30
     *                       },
     *      user2_metadata : {
     *                          requests: 150, 
     *                          window_time: 35
     *                       },
     * }
     * 
     * This is the HMSET (hash data-structure) that is used to store the user's metadata as a HashMap.
     */
    private static final String METADATA_SUFFIX = "_metadata";

    /*
     * This is a suffix that is appended to the user id to store the user's requests timestamps.
     * 
     *      "userid_timestamps": sorted_set([
	 *          "ts1": "ts1",
	 *          "ts2": "ts2"
	 *      ])
     * 
     * This is the ZADD (sorted set data-structure) that is used to store the user's requests timestamps.
     */
    private static final String TIMESTAMPS_SUFFIX = "_timestamps";

    public static void main(String[] args) throws Exception {
        SlidingWindowLogRatelimiter slidingWindowLogRatelimiter = new SlidingWindowLogRatelimiter();
        slidingWindowLogRatelimiter.addUser("user1");
        long currentTimeInMillis = System.currentTimeMillis();
        for (int i = 0; i < 200; i++) {
            System.out.println("===========================================================================");
            System.out.println("Time taken: " + (System.currentTimeMillis() - currentTimeInMillis) / 1000);
            boolean isAllowed = slidingWindowLogRatelimiter.shouldAllowServiceCall("user1");
            if (!isAllowed) {
                System.out.println("Service call throttled. Sleeping for 5 seconds");
                Thread.sleep(5000);
            }
        }

    }

    public SlidingWindowLogRatelimiter() {
        // initializing redis connection.
        initializeRedisConnection();
    }

    /*
     * This method is used to initialize the redis connection. We use Jedis client to connect to redis.
     * Also, the underlying Redis cache is an Azure Cache for Redis.
     * https://portal.azure.com/#@mankadnandangmail.onmicrosoft.com/resource/subscriptions/c2f61104-d775-42c1-8fc2-9843f14aae93/resourceGroups/distributedratelimiter/providers/Microsoft.Cache/Redis/distributedratelimiter/overview
     */
    private void initializeRedisConnection() {
        boolean useSsl = false;
        String cacheHostname = "distributedratelimiter.redis.cache.windows.net";
        String cachekey = "km2dzw4cA7mTOQOE0xPd1vckXgDZIo0V0AzCaFsn7q8=";
        jedis = new Jedis(cacheHostname, 6379, DefaultJedisClientConfig.builder()
                .password(cachekey)
                .ssl(useSsl)
                .build());
    }

    /*
     * This method is used to add a new User to the rate limiter. Expectation is that any user that requires
     * the rate limiter functionality should first be added through this method.
     * This method add the user with the default rate of 100 requests per 30 seconds.
     */
    public void addUser(String userId) {
        addUser(userId, 100, 30);
    }

    /*
     * This method is an overloaded version of the addUser method. It allows the user to specify the rate limit parameters.
     */
    public void addUser(String userId, int requests, int windowInSeconds) {
        Map<String, String> map = new HashMap<String, String>();
        map.put(REQUESTS, String.valueOf(requests));
        map.put(WINDOW_TIME, String.valueOf(windowInSeconds));
        jedis.hmset(userId + METADATA_SUFFIX, map);
    }

    /*
     * This method allows to remove the already added user to the rate limiter.
     */
    public void removeUser(String userId) {
        jedis.del(userId + METADATA_SUFFIX);
        jedis.del(userId + TIMESTAMPS_SUFFIX);
    }

    /*
     * This method is used to decide if the incoming request should be allowed or not. 
     * Upon receiving the request, it does the following to verify it this request should be allowed or should be throttled:
     * 1. Get the user's metadata from the cache. This gives the rate limit parameters for the user.
     * 2. The parameters are stored in maxRequests and windowInSeconds variables.
     * 3. We then calculate the oldest possible entry that should be present in the sorted set. This is done by subtracting 
     *    the windowInSeconds from the current time.
     * 4. We then remove all the entries from the sorted set that are older than the oldest possible entry. We do this to ensure we only keep the entries
     *    that are relevant for the current window of time.
     * 5. We then add the current timestamp to the sorted set. This is done atomically using the ZADD command.
     * 6. We then get the current size of the sorted set. This gives us the number of requests that have been made in the current window of time.
     * 7. We then compare the current size of the sorted set with the maxRequests. If the current size is greater than the maxRequests, we return false
     *    which means we throttle the request. Else, we return true which means we allow the request.
     */
    public boolean shouldAllowServiceCall(String userId) {
        Map<String, String> map = getRateForUser(userId);
        if (map == null || map.isEmpty()) {
            return false;
        }
        int maxRequests = Integer.parseInt(map.get(REQUESTS));
        int windowInSeconds = Integer.parseInt(map.get(WINDOW_TIME));
        long currentTimeInMillis = System.currentTimeMillis();
        long oldestPossibleEntry = currentTimeInMillis - (windowInSeconds * 1000);
        long removedCount = jedis.zremrangeByScore(userId + TIMESTAMPS_SUFFIX, "0", String.valueOf(oldestPossibleEntry));
        System.out.println("removedCount: " + removedCount);
        int currentReqCount = addTimeStampAtomicallyAndReturnSize(userId, currentTimeInMillis);
        System.out.println("currentReqCount: " + currentReqCount);
        System.out.println("maxRequests: " + maxRequests);
        if (currentReqCount > maxRequests) {
            return false;
        }
        return true;
    }

    /*
     * This method is used to get the rate limit parameters for a user.
     */
    private Map<String, String> getRateForUser(String userId) {
        Map<String, String> map = jedis.hgetAll(userId + METADATA_SUFFIX);
        return map;
    }

    /*
     * This method is used to add the current timestamp to the sorted set. This is done atomically using the ZADD command.
     * We then get the current size of the sorted set. This gives us the number of requests that have been made in the current window of time.
     * 
     * It is important to take a note of the fact that we are using a MULTI/EXEC block to ensure that the ZADD and ZCARD 
     * commands are executed atomically. This is important when we have multiple threads trying to access the rate limiter for the same user.
     * 
     * Also, we are using WATCH command on the sorted set and the hash map. This is to ensure we use optimistic locks on these two data-strucutres 
     * to allow them to be updated by only one thread at a time. This is important to ensure that the rate limiter works correctly in a multi-threaded environment.
     */
    private int addTimeStampAtomicallyAndReturnSize(String userId, long timestamp) {
        try (Transaction transaction = jedis.multi()) {

            // Watch the keys for optimistic locking
            transaction.watch(userId + METADATA_SUFFIX, userId + TIMESTAMPS_SUFFIX);
            addNewTimestampAndReturnTotalCount(userId, timestamp, transaction);
            // Execute the transaction with the lambda function (as a Java 8 lambda
            // expression)
            List<Object> response = transaction.exec();

            // Process the response
            if (response == null) {
                // Transaction was aborted
                return -1; // Or handle the conflict appropriately
            } else {
                // Transaction was successful, process the result
                Long totalCount = (Long) response.get(1);
                return totalCount.intValue();
            }
        } catch (Exception e) {
            // Handle exceptions here (e.g., WatchFailException)
            e.printStackTrace();
            return -1; // Or return an appropriate value for failure
        }
    }

    /* 
     * This method is used to add the current timestamp to the sorted set and also return the total number of entries in the sorted set
     * after the addition of the timestamp of the current request.
     */
    private void addNewTimestampAndReturnTotalCount(String userId, long timestamp, Transaction transaction) {
        transaction.zadd(userId + TIMESTAMPS_SUFFIX, timestamp, String.valueOf(timestamp));
        transaction.zcard(userId + TIMESTAMPS_SUFFIX);
    }
}