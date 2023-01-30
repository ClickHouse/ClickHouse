-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query result cache (QRC) and query log
SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE system.query_log SYNC;

-- Run a query with QRC on. The first execution is a QRC miss.
SELECT '---';
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryResultCacheHits'], ProfileEvents['QueryResultCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS enable_experimental_query_result_cache = true;';


-- Run previous query again with query result cache on
SELECT '---';
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;

DROP TABLE system.query_log SYNC;
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryResultCacheHits'], ProfileEvents['QueryResultCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS enable_experimental_query_result_cache = true;';

SYSTEM DROP QUERY RESULT CACHE;
