
-- Start with empty query cache (QC)
SET query_cache_tag = '02494_query_cache_case_agnostic_matching';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_case_agnostic_matching';

-- Insert an entry into the query cache.
SELECT 1 SETTINGS use_query_cache = true;
-- Check that entry in QC exists
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_case_agnostic_matching') AS test_query_cache;

-- Run the same SELECT but with different case (--> select). We want its result to be served from the QC.
SELECT '---';
select 1 SETTINGS use_query_cache = true;

-- There should still be just one entry in the QC
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_case_agnostic_matching') AS test_query_cache;

-- The second query should cause a QC hit.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'select 1 SETTINGS use_query_cache = true;';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_case_agnostic_matching';
