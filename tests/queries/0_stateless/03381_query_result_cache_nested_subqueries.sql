-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Creates 3 records in query result cache
SELECT number FROM numbers(1, 100)
WHERE number IN (
    SELECT number + (SELECT ceil(avg(number)) FROM numbers(1, 100)) FROM numbers(1, 10)
)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Should be 3 records (main query, sub-query and nested sub-query)
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

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

SYSTEM DROP QUERY CACHE;