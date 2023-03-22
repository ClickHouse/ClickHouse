-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_query_cache = true;

-- The test assumes that these two settings have default values. Neutralize the effect of setting randomization:
SET use_query_cache = false;
SET enable_reads_from_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- If an exception is thrown during query execution, no entry must be created in the query cache
SELECT throwIf(1) SETTINGS use_query_cache = true; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
