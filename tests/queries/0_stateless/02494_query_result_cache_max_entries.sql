-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- store one entry into query result cache
SELECT 1 SETTINGS experimental_query_result_cache_active_usage = true;
SELECT count(*) FROM system.queryresult_cache;

-- restrict entry count to 1, other queries create no further entries
SET query_result_cache_max_entries = 1;
SELECT 2 SETTINGS experimental_query_result_cache_active_usage = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
