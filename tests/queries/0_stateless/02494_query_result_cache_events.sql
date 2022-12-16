-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE IF EXISTS old;

-- save current event counts for query result cache
CREATE TABLE old (event String, value UInt64) ENGINE=MergeTree ORDER BY event;
INSERT INTO old SELECT event, value FROM system.events WHERE event LIKE 'QueryResultCache%';

-- Run a query with query result cache disabled, the event counts should not change
SELECT 1;

SELECT value = (SELECT value FROM old WHERE event = 'QueryResultCacheHits')
FROM system.events
WHERE event = 'QueryResultCacheHits';

SELECT value = (SELECT value FROM old WHERE event = 'QueryResultCacheMisses')
FROM system.events
WHERE event = 'QueryResultCacheMisses';

-- Run a query with query result cache on, the miss count increments +1
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;

SELECT value = (SELECT value FROM old WHERE event = 'QueryResultCacheHits')
FROM system.events
WHERE event = 'QueryResultCacheHits';

SELECT value = (SELECT value FROM old WHERE event = 'QueryResultCacheMisses') + 1
FROM system.events
WHERE event = 'QueryResultCacheMisses';

-- Run previous query again with query result cache on, the hit count increments +1
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;

SELECT value = (SELECT value FROM old WHERE event = 'QueryResultCacheHits') + 1
FROM system.events
WHERE event = 'QueryResultCacheHits';

SELECT value = (SELECT value FROM old WHERE event = 'QueryResultCacheMisses') + 1
FROM system.events
WHERE event = 'QueryResultCacheMisses';

SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE old;

-- { echoOff }
