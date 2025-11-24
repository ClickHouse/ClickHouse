--- Tags: no-parallel
--- Tag no-parallel: Messes with internal cache

-- Check that setting 'log_comment' is ignored in query cache lookups

SET query_cache_ignore_certain_non_query_cache_settings = 1;

SYSTEM DROP QUERY CACHE;

SELECT 1 SETTINGS use_query_cache = 1, log_comment='aaa' FORMAT Null;
SELECT 1 SETTINGS use_query_cache = 1, log_comment='bbb' FORMAT Null;
SELECT 1 SETTINGS use_query_cache = 1, log_comment='aaa' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
    AND event_time > now() - 600
    AND current_database = currentDatabase()
    AND query LIKE 'SELECT 1 SETTINGS use_query_cache%'
ORDER BY event_time_microseconds;

-- Now check again but with legacy settings
SET query_cache_ignore_certain_non_query_cache_settings = 0;

SYSTEM DROP QUERY CACHE;

SELECT 2 SETTINGS use_query_cache = 1, log_comment='aaa' FORMAT Null;
SELECT 2 SETTINGS use_query_cache = 1, log_comment='bbb' FORMAT Null;
SELECT 2 SETTINGS use_query_cache = 1, log_comment='aaa' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
    AND event_time > now() - 600
    AND current_database = currentDatabase()
    AND query LIKE 'SELECT 2 SETTINGS use_query_cache%'
ORDER BY event_time_microseconds;

SYSTEM DROP QUERY CACHE;
