-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Create an entry in the query result cache
SELECT t1.number, t2.avg FROM numbers(1, 100) as t1 JOIN (SELECT toUInt64(ceil(avg(number))) as avg FROM numbers(1, 100)) AS t2 ON t1.number = t2.avg SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Should be two records (main query and join subquery)
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

-- SELECT with sub-query result already in cache
SELECT t1.number + 1, t2.avg FROM numbers(1, 100) as t1 JOIN (SELECT toUInt64(ceil(avg(number))) as avg FROM numbers(1, 100)) AS t2 ON t1.number = t2.avg SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

-- Check CacheHit
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%SELECT t1.number + 1, t2.avg FROM numbers(1, 100) as t1 JOIN (SELECT toUInt64(ceil(avg(number))) as avg FROM numbers(1, 100)) AS t2 ON t1.number = t2.avg SETTINGS use_query_cache = true, query_cache_for_subqueries = true;%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP QUERY CACHE;