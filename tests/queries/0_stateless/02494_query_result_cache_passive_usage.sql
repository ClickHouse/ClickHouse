-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- by default don't write query result into cache
SELECT 1;
SELECT count(*) FROM system.queryresult_cache;

-- try to retrieve from empty cache, cache should still be empty
SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true;
SELECT count(*) FROM system.queryresult_cache;

-- put query result into cache
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- read from query result cache
SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
