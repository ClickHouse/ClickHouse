-- Tags: no-parallel

SYSTEM DROP QUERY RESULT CACHE;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 0;
SELECT COUNT(*) FROM system.queryresult_cache;

SELECT '---';

SYSTEM DROP QUERY RESULT CACHE;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 1;
SELECT COUNT(*) FROM system.queryresult_cache;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 1;
SELECT COUNT(*) FROM system.queryresult_cache;

SELECT '---';

SYSTEM DROP QUERY RESULT CACHE;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.queryresult_cache;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.queryresult_cache;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;
