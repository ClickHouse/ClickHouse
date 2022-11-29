-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- this does not create a cache entry ...
SELECT 1 SETTINGS experimental_query_result_cache_active_usage = true, query_result_cache_max_entry_size = 0;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- ... but this does
SELECT 1 SETTINGS experimental_query_result_cache_active_usage = true, query_result_cache_max_entry_size = 999999999;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
