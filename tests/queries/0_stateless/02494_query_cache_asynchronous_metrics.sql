-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Create an entry in the query cache
SELECT 1 SETTINGS use_query_cache = true;

-- Asynchronous metrics must know about the entry
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT value FROM system.asynchronous_metrics WHERE metric = 'QueryCacheEntries';

SYSTEM DROP QUERY CACHE;
