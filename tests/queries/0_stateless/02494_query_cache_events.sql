
-- Start with empty query cache QC
SET query_cache_tag = '02494_query_cache_events';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_events';

SELECT 1 SETTINGS use_query_cache = true;
SELECT 1 SETTINGS use_query_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT 1 SETTINGS use_query_cache = true;'
ORDER BY event_time_microseconds;

-- (The 1st execution was a cache miss, the 2nd execution was a cache hit)

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_events';
