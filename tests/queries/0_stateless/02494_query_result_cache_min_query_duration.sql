-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- this does create an entry in the query result cache
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- ... but this does not because the query executes much faster than the milliseconds threshold
SET query_result_cache_max_entry_size = 10000;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
