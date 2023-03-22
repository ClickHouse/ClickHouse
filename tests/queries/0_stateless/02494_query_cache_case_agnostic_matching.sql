-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_query_cache = true;

-- Start with empty query cache (QC) and query log
SYSTEM DROP QUERY CACHE;
DROP TABLE system.query_log SYNC;

-- Insert an entry into the query cache.
SELECT 1 SETTINGS use_query_cache = true;
-- Check that entry in QC exists
SELECT COUNT(*) FROM system.query_cache;

-- Run the same SELECT but with different case (--> select). We want its result to be served from the QC.
SELECT '---';
select 1 SETTINGS use_query_cache = true;

-- There should still be just one entry in the QC
SELECT COUNT(*) FROM system.query_cache;

-- The second query should cause a QC hit.
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'select 1 SETTINGS use_query_cache = true;';

SYSTEM DROP QUERY CACHE;
