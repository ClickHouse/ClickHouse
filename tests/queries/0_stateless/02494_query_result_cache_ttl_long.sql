-- Tags: no-fasttest, no-parallel, long
-- Tag no-fasttest: Test runtime is > 6 sec
-- Tag long: Test runtime is > 6 sec
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY RESULT CACHE;

-- Cache query result into query result cache with a TTL of 3 sec
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_ttl = 3;

-- Expect one non-stale cache entry
SELECT COUNT(*) FROM system.query_result_cache;
SELECT stale FROM system.query_result_cache;

-- Wait until entry is expired
SELECT sleep(3);
SELECT sleep(3);
SELECT stale FROM system.query_result_cache;

SELECT '---';

-- Run same query as before
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_ttl = 3;

-- The entry should have been refreshed (non-stale)
SELECT COUNT(*) FROM system.query_result_cache;
SELECT stale FROM system.query_result_cache;

SYSTEM DROP QUERY RESULT CACHE;
