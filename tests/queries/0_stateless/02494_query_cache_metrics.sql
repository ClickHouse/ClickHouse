-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Create an entry in the query cache
SELECT 1 SETTINGS use_query_cache = true FORMAT Null;

SELECT metric, value FROM system.metrics WHERE metric = 'QueryCacheEntries';

SYSTEM DROP QUERY CACHE;
