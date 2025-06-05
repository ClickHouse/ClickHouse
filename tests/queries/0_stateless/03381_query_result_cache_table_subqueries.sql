-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Put query into cache
SELECT ceil(avg(number)) FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Should be one record in system.query_cache with is_subquery = 1 
SELECT * FROM system.query_cache WHERE is_subquery = 1 ORDER BY expires_at;

-- SELECT with sub-query result in cache
SELECT * FROM (SELECT ceil(avg(number)) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Should be one record in system.query_cache with is_subquery = 1 
SELECT * FROM system.query_cache WHERE is_subquery = 1 ORDER BY expires_at;

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