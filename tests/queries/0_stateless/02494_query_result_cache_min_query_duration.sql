-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- this does not create a cache entry ...
SET query_result_cache_max_entry_size = 0;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- ... but this does
SET query_result_cache_max_entry_size = 9999999;
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
