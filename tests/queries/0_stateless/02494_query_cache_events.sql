-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query cache QC
SYSTEM DROP QUERY CACHE;

SELECT 1 SETTINGS use_query_cache = true;
SELECT 1 SETTINGS use_query_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT 1 SETTINGS use_query_cache = true;'
ORDER BY event_time_microseconds;

-- (The 1st execution was a cache miss, the 2nd execution was a cache hit)

SYSTEM DROP QUERY CACHE;
