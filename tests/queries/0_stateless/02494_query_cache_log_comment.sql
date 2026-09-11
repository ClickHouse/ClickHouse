
-- Check that setting 'log_comment' is ignored in query cache lookups

SET query_cache_tag = '02494_query_cache_log_comment';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_log_comment';

SELECT 1 SETTINGS use_query_cache = 1, log_comment='aaa' FORMAT Null;
SELECT 1 SETTINGS use_query_cache = 1, log_comment='bbb' FORMAT Null;
SELECT 1 SETTINGS use_query_cache = 1, log_comment='aaa' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND event_time > now() - 600
    AND current_database = currentDatabase()
    AND query LIKE 'SELECT 1 SETTINGS use_query_cache%'
ORDER BY event_time_microseconds;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_log_comment';
