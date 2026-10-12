-- Tags: no-parallel
-- Tag no-parallel: asserts the exact value of the process-global `QueryCacheEntries` gauge in
-- `system.metrics`, which is not scoped by tag or database; concurrent query-cache activity from
-- other tests would change the count.

SYSTEM CLEAR QUERY CACHE;

-- Create an entry in the query cache
SELECT 1 SETTINGS use_query_cache = true FORMAT Null;

SELECT metric, value FROM system.metrics WHERE metric = 'QueryCacheEntries';

SYSTEM CLEAR QUERY CACHE;
