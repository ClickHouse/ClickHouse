-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query result cache (QRC) and query log
SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE system.query_log SYNC;

-- Insert an entry into the query result cache.
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
-- Check that entry in QRC exists
SELECT COUNT(*) FROM system.query_result_cache;

-- Run the same SELECT but with different case (--> select). We want its result to be served from the QRC.
SELECT '---';
select 1 SETTINGS enable_experimental_query_result_cache = true;

-- There should still be just one entry in the QRC
SELECT COUNT(*) FROM system.query_result_cache;

-- The second query should cause a QRC hit.
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryResultCacheHits'], ProfileEvents['QueryResultCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'select 1 SETTINGS enable_experimental_query_result_cache = true;';

SYSTEM DROP QUERY RESULT CACHE;
