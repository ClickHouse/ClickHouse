
SET query_cache_tag = '02494_query_cache_query_log';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_query_log';

-- DROP TABLE system.query_log; -- debugging



SELECT '-- Run a query with query cache not enabled';
SELECT 124437993;

SYSTEM FLUSH LOGS query_log;

-- Field 'query_cache_usage' should be 'None'
SELECT type, query, query_cache_usage
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND query = 'SELECT 124437993;'
    AND type = 'QueryFinish'
ORDER BY type, query_cache_usage;



SELECT '-- Run a query with query cache enabled';
SELECT 124437994 SETTINGS use_query_cache = 1;

SYSTEM FLUSH LOGS query_log;

-- Field 'query_cache_usage' should be 'Write'
SELECT type, query, query_cache_usage
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND query = 'SELECT 124437994 SETTINGS use_query_cache = 1;'
    AND type = 'QueryFinish'
ORDER BY type, query_cache_usage;



SELECT '-- Run the same query with query cache enabled';
SELECT 124437994 SETTINGS use_query_cache = 1;
SELECT 124437994 SETTINGS use_query_cache = 1;
SELECT 124437994 SETTINGS use_query_cache = 1;

SYSTEM FLUSH LOGS query_log;

-- The query cache is process-wide and size-limited, so a concurrently running test can evict this
-- entry between two runs of the query, and the next run then legitimately writes it again instead
-- of reading it. Pinning the usage of one particular run is therefore not observable, so assert
-- across the repeats above the cache was both written and read at least once.
SELECT countIf(query_cache_usage = 'Write') > 0, countIf(query_cache_usage = 'Read') > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND query = 'SELECT 124437994 SETTINGS use_query_cache = 1;'
    AND type = 'QueryFinish';



SELECT '-- Throw exception with query cache enabled';
SELECT 124437995, throwIf(1) SETTINGS use_query_cache = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SYSTEM FLUSH LOGS query_log;

-- Field 'query_cache_usage' should be 'None'
SELECT query, query_cache_usage
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND query = 'SELECT 124437995, throwIf(1) SETTINGS use_query_cache = 1;'
    AND type = 'ExceptionWhileProcessing';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_query_log';
