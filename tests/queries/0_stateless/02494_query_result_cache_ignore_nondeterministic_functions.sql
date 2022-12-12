-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- rand() is non-deterministic, with default settings no entry in the query result cache should be created
SELECT count(rand(1)) SETTINGS experimental_query_result_cache_active_usage = true;
SELECT count(*) FROM system.queryresult_cache;

SELECT count(rand(1)) SETTINGS experimental_query_result_cache_active_usage = true, query_result_cache_ignore_nondeterministic_functions = false;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
