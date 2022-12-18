-- Tags: no-fasttest, no-parallel

SYSTEM DROP QUERY RESULT CACHE;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_keep_seconds_alive = 3;

-- expect one non-stale cache entry
SELECT COUNT(*) FROM system.queryresult_cache;
SELECT stale FROM system.queryresult_cache;

SELECT sleep(3);
SELECT sleep(3);

-- cache entry is stale by now
SELECT stale FROM system.queryresult_cache;

SELECT '---';

-- same query as before
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_keep_seconds_alive = 3;

-- expect one non-stale cache entry
SELECT COUNT(*) FROM system.queryresult_cache;
SELECT stale FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;
