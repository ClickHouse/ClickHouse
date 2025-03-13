-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Put only main query result into cache
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true;

-- Should be 0 records in system.query_cache with is_subquery = 1 
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

SYSTEM DROP QUERY CACHE; -- Drop QC

-- QC for sub-queries results works only with use_query_cache = true
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS query_cache_for_subqueries = true;

-- Should be 0 records in system.query_cache
SELECT count(*) FROM system.query_cache;

-- Overriding QC setting in sub-query
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS enable_writes_to_query_cache = true)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, enable_writes_to_query_cache = false;

-- No result for main query in QC
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS enable_writes_to_query_cache = true)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, enable_writes_to_query_cache = false;

-- Check CacheMisses for main query and CacheHit for sub-query
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS enable_writes_to_query_cache = true)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, enable_writes_to_query_cache = false;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Result in QC
SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Check CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;

-- Store result only for main query
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS query_cache_for_subqueries = false)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- One record (main query)
SELECT count(*) FROM system.query_cache WHERE is_subquery = true;

SYSTEM DROP QUERY CACHE;