-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Put query into cache
SELECT * FROM (SELECT avg(number) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Should be 2 records in system.query_cache with is_subquery = 1 
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

-- Same query, but with alias in sub-query
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Check no CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Should be 4 records in system.query_cache with is_subquery = 1 
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

SYSTEM DROP QUERY CACHE;