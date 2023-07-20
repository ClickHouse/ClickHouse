-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- DROP TABLE system.query_log; -- debugging



SELECT '-- Run a query with query cache not enabled';
SELECT 124437993;

SYSTEM FLUSH LOGS;

-- Field 'query_cache_usage' should be 'None'
SELECT type, query, query_cache_usage
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query = 'SELECT 124437993;'
ORDER BY type;



SELECT '-- Run a query with query cache enabled';
SELECT 124437994 SETTINGS use_query_cache = 1;

SYSTEM FLUSH LOGS;

-- Field 'query_cache_usage' should be 'Write'
SELECT type, query, query_cache_usage
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query = 'SELECT 124437994 SETTINGS use_query_cache = 1;'
ORDER BY type;



SELECT '-- Run the same query with query cache enabled';
SELECT 124437994 SETTINGS use_query_cache = 1;

SYSTEM FLUSH LOGS;

-- Field 'query_cache_usage' should be 'Read'
SELECT type, query, query_cache_usage
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query = 'SELECT 124437994 SETTINGS use_query_cache = 1;'
ORDER BY type;



SELECT '-- Throw exception with query cache enabled';
SELECT 124437995, throwIf(1) SETTINGS use_query_cache = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SYSTEM FLUSH LOGS;

-- Field 'query_cache_usage' should be 'None'
SELECT query, query_cache_usage
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query = 'SELECT 124437995, throwIf(1) SETTINGS use_query_cache = 1;'
    AND type = 'ExceptionWhileProcessing';

SYSTEM DROP QUERY CACHE;
