-- Tags: no-fasttest, no-parallel, long
-- Tag no-fasttest: Test runtime is > 6 sec
-- Tag long: Test runtime is > 6 sec
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Cache query result into query cache with a TTL of 3 sec
SELECT 1 SETTINGS use_query_cache = true, query_cache_ttl = 3;

-- Expect one non-stale cache entry
SELECT COUNT(*) FROM system.query_cache;
SELECT stale FROM system.query_cache;

-- Wait until entry is expired
SELECT sleep(3);
SELECT sleep(3);
SELECT stale FROM system.query_cache;

SELECT '---';

-- Run same query as before
SELECT 1 SETTINGS use_query_cache = true, query_cache_ttl = 3;

-- The entry should have been refreshed (non-stale)
SELECT COUNT(*) FROM system.query_cache;
SELECT stale FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
