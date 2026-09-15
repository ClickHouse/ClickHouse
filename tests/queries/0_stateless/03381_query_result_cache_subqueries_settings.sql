
SET query_cache_tag = '03381_query_result_cache_subqueries_settings';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_settings';

-- Put only main query result into cache
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true;

-- Should be 0 records in system.query_cache with is_subquery = 1 
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_subqueries_settings') AS test_query_cache WHERE is_subquery = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_settings'; -- Drop QC

-- QC for sub-queries results works only with use_query_cache = true
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS query_cache_for_subqueries = true;

-- Should be 0 records in system.query_cache
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_subqueries_settings') AS test_query_cache;

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
  AND current_database = currentDatabase()
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
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_settings';

-- Store result only for main query
SELECT * FROM (SELECT avg(number) as avg FROM numbers(1, 100) SETTINGS query_cache_for_subqueries = false)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- One record (main query)
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_subqueries_settings') AS test_query_cache WHERE is_subquery = true;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_settings';
