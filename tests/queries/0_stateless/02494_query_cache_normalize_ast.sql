-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query cache (QC)
SYSTEM DROP QUERY CACHE;

-- First, run a query whose result gets cached in the query cache.
-- Besides "use_query_cache", pass two more knobs (one QC-specific knob and one non-QC-specific knob). We just care
-- *that* they are passed and not about their effect.
SELECT 1 SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save', max_threads = 16;

-- Check that entry in QC exists
SELECT COUNT(*) FROM system.query_cache;

-- Run the same SELECT but with different SETTINGS. We want its result to be served from the QC (--> passive mode, achieve it by
-- disabling active mode)
SELECT '---';
SELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false, max_threads = 16;

-- Technically, both SELECT queries have different ASTs, leading to different QC keys. QC does some AST normalization (erase all
-- QC-related settings) such that the keys match regardless. Verify by checking that the second query caused a QC hit.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false, max_threads = 16;';

SYSTEM DROP QUERY CACHE;

SELECT '--- ---';
-- We'll repeat the fun with setting `log_comment` to check that such settings are ignored (similar to query cache related settings).

-- First, run a query whose result gets cached in the query cache.
-- Run the query with a log_comment and another settings.

SET use_query_cache = 1; -- Enable caching via SET so the test is more targeted.
SELECT 1 SETTINGS log_comment= 'hello', max_threads = 16;

-- Check that entry in QC exists
SET use_query_cache = 0; -- temporarily disable
SELECT COUNT(*) FROM system.query_cache;

-- Run the same SELECT but with a different log_comment.
-- We want its result to be served from the QC (--> passive mode, achieve it by disabling active mode).
SELECT '---';
SET use_query_cache = 1; -- enable again
SELECT 1 SETTINGS log_comment= 'world', enable_writes_to_query_cache = false, max_threads = 16;

-- Technically, both SELECT queries have different ASTs, leading to different QC keys. QC does some AST normalization (erase all
-- QC-related settings) such that the keys match regardless. Verify by checking that the second query caused a QC hit.
SET use_query_cache = 0;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'hello';

SYSTEM DROP QUERY CACHE;
