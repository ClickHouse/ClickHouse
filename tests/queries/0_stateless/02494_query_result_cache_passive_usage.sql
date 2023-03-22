-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query result cache (QRC).
SYSTEM DROP QUERY RESULT CACHE;

-- By default, don't write query result into query result cache (QRC).
SELECT 1;
SELECT COUNT(*) FROM system.query_result_cache;

SELECT '-----';

-- Try to retrieve query result from empty QRC using the passive mode. The cache should still be empty (no insert).
SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true;
SELECT COUNT(*) FROM system.query_result_cache;

SELECT '-----';

-- Put query result into cache.
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT COUNT(*) FROM system.query_result_cache;

SELECT '-----';

-- Run same query with passive mode again. There must still be one entry in the QRC and we must have a QRC hit.

-- Get rid of log of previous SELECT
DROP TABLE system.query_log SYNC;

SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true;
SELECT COUNT(*) FROM system.query_result_cache;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryResultCacheHits'], ProfileEvents['QueryResultCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true;';

SYSTEM DROP QUERY RESULT CACHE;
