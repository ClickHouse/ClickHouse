-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- Test 1: Explicit opt-in on subquery creates cache entry with is_subquery = 1
SELECT * FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true) ORDER BY number;

SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1

-- Test 2: Second run hits cache (verified via ProfileEvents)
SELECT * FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true) ORDER BY number;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT number FROM numbers(5) SETTINGS use_query_cache = true%'
  AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;
