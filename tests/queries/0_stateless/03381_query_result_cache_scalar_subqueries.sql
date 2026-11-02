-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- Cache the top-level query (under is_subquery = 0)
SELECT ceil(avg(number)) FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- No Planner-level (is_subquery = 1) entries from a top-level query
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

-- SELECT with scalar sub-query result already in cache
SELECT number, (SELECT ceil(avg(number)) FROM numbers(1, 100)) as scalar_value FROM numbers(1, 3) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Check CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT number, (SELECT ceil(avg(number)) FROM numbers(1, 100)) as scalar_value FROM numbers(1, 3) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;
