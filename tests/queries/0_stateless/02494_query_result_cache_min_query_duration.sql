-- Tags: no-parallel

SYSTEM DROP QUERY RESULT CACHE;

-- this does create an entry in the query result cache
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

SELECT '---';

-- ... but this does not because the query executes much faster than the milliseconds threshold
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_duration = 10000;
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;
