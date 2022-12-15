-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- this does create an entry in the query result cache
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- ... but this not because the query executes much faster than the milliseconds threshold
SET query_result_cache_min_query_duration = 10000;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
