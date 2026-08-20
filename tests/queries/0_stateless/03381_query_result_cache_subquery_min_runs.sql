-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- Test: query_cache_min_query_runs works for subqueries
-- With min_query_runs = 2, the first run should not create a cache entry.

-- First run: no cache entry yet (min_query_runs not met)
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true, query_cache_min_query_runs = 2) ORDER BY number;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 0

-- Second run: cache entry created (min_query_runs met)
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true, query_cache_min_query_runs = 2) ORDER BY number;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1

-- Third run: cache hit
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true, query_cache_min_query_runs = 2) ORDER BY number;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT number FROM numbers(5) SETTINGS use_query_cache = true, query_cache_min_query_runs = 2%'
  AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;
