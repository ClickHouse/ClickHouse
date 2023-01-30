-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_query_result_cache = true;

-- Start with empty query result cache (QRC).
SYSTEM DROP QUERY RESULT CACHE;

-- By default, don't write query result into query result cache (QRC).
SELECT 1;
SELECT COUNT(*) FROM system.query_result_cache;

SELECT '-----';

-- Try to retrieve query result from empty QRC using the passive mode. Do this by disabling the active mode. The cache should still be empty (no insert).
SELECT 1 SETTINGS use_query_result_cache = true, enable_writes_to_query_result_cache = false;
SELECT COUNT(*) FROM system.query_result_cache;

SELECT '-----';

-- Put query result into cache.
SELECT 1 SETTINGS use_query_result_cache = true;
SELECT COUNT(*) FROM system.query_result_cache;

SELECT '-----';

-- Run same query with passive mode again. There must still be one entry in the QRC and we must have a QRC hit.

-- Get rid of log of previous SELECT
DROP TABLE system.query_log SYNC;

SELECT 1 SETTINGS use_query_result_cache = true, enable_writes_to_query_result_cache = false;
SELECT COUNT(*) FROM system.query_result_cache;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryResultCacheHits'], ProfileEvents['QueryResultCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS use_query_result_cache = true, enable_writes_to_query_result_cache = false;';

SYSTEM DROP QUERY RESULT CACHE;
