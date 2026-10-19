
SET query_cache_tag = '03381_query_result_cache_nested_subqueries';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_nested_subqueries';

-- Creates 3 records in query result cache
SELECT number FROM numbers(1, 100)
WHERE number IN (
    SELECT number + (SELECT ceil(avg(number)) FROM numbers(1, 100)) FROM numbers(1, 10)
)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Should be 2 records (sub-query and nested sub-query)
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_nested_subqueries') AS test_query_cache WHERE is_subquery = 1;

-- SELECT with sub-query result already in cache
SELECT number + (SELECT ceil(avg(number)) FROM numbers(1, 100)) FROM numbers(1, 10) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Check CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT number + (SELECT ceil(avg(number)) FROM numbers(1, 100)) FROM numbers(1, 10) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- SELECT with nested sub-query result already in cache
SELECT ceil(avg(number)) FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Check CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT ceil(avg(number)) FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_nested_subqueries';
