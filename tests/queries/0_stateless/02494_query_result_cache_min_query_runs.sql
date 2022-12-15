-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 0;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 1;
SELECT count(*) FROM system.queryresult_cache;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 1;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 2;
SELECT count(*) FROM system.queryresult_cache;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 2;
SELECT count(*) FROM system.queryresult_cache;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_runs = 2;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
