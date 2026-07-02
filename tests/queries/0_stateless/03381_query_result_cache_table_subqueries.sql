-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- Put query into cache (top-level query, cached under is_subquery = 0)
SELECT ceil(avg(number)) FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- No subquery entries yet: the previous query is top-level (`is_subquery = 0`)
SELECT query, is_subquery FROM system.query_cache WHERE is_subquery = 1 ORDER BY query;

-- SELECT with table sub-query, exercising the Planner-level cache path
SELECT * FROM (SELECT ceil(avg(number)) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Now exactly one Planner-level entry (the inner `SELECT ceil(avg(number)) FROM numbers(1, 100)`)
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

-- Check CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT * FROM (SELECT ceil(avg(number)) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;
