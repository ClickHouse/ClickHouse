-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query cache QC
SYSTEM DROP QUERY CACHE;

-- Run a query with QC on. The first execution is a QC miss.
SELECT '---';
SELECT 1 SETTINGS use_query_cache = true;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT 1 SETTINGS use_query_cache = true;';


-- Run previous query again with query cache on
SELECT '---';
SELECT 1 SETTINGS use_query_cache = true;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT 1 SETTINGS use_query_cache = true;'
ORDER BY event_time_microseconds;

SYSTEM DROP QUERY CACHE;
