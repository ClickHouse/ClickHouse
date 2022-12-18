-- Tags: no-parallel

SYSTEM DROP QUERY RESULT CACHE;

-- by default don't write query result into query result cache
SELECT 1;
SELECT COUNT(*) FROM system.queryresult_cache;

SELECT '-----';

-- try to retrieve from empty cache, cache should still be empty
SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SELECT '-----';

-- put query result into cache
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SELECT '-----';

-- read from query result cache
SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;
