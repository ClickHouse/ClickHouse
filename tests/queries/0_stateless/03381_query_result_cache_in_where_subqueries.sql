
SET query_cache_tag = '03381_query_result_cache_in_where_subqueries';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_in_where_subqueries';

-- Create a top-level entry (cached under is_subquery = 0)
SELECT ceil(avg(number)) FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- No Planner-level (is_subquery = 1) entries yet; the previous query is top-level
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_in_where_subqueries') AS test_query_cache WHERE is_subquery = 1;

-- SELECT with sub-query result already in cache
SELECT number FROM numbers(1, 100) WHERE number IN (SELECT ceil(avg(number)) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Check CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT number FROM numbers(1, 100) WHERE number IN (SELECT ceil(avg(number)) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_in_where_subqueries';
