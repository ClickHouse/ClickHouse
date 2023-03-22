-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache
-- Tag no-random-settings: Assumes default settings for the query cache.

SET allow_experimental_query_cache = true;

-- The test assumes that these two settings have default values. Neutralize the effect of setting randomization:
SET use_query_cache = false;
SET enable_reads_from_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- rand() is non-deterministic, with default settings no entry in the query cache should be created
SELECT COUNT(rand(1)) SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

-- But an entry can be forced using a setting
SELECT COUNT(RAND(1)) SETTINGS use_query_cache = true, query_cache_store_results_of_queries_with_nondeterministic_functions = true;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
