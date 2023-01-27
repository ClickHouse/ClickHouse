-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query result cache (QRC) and query log.
SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE system.query_log SYNC;

-- Run query whose result gets cached in the query result cache.
-- Besides "enable_experimental_query_result_cache", pass two more knobs (one QRC-specific knob and one non-QRC-specific knob). We just care
-- *that* they are passed and not about their effect.
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_store_results_of_queries_with_nondeterministic_functions = true, max_threads = 16;

-- Check that entry in QRC exists
SELECT COUNT(*) FROM system.query_result_cache;

-- Run the same SELECT but with different SETTINGS. We want its result to be served from the QRC.
SELECT '---';
SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true, max_threads = 16;

-- Technically, both SELECT queries have different ASTs, leading to different QRC keys. QRC does some AST normalization (erase all
-- QRC-related settings) such that the keys match regardless. Verify by checking that the second query caused a QRC hit.
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryResultCacheHits'], ProfileEvents['QueryResultCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true, max_threads = 16;';

SYSTEM DROP QUERY RESULT CACHE;
