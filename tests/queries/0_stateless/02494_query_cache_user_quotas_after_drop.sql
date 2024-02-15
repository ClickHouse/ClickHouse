-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests per-user quotas of the query cache. Settings 'query_cache_max_size_in_bytes' and 'query_cache_max_entries' are actually supposed to
-- be used in a settings profile, together with a readonly constraint. For simplicity, test both settings stand-alone in a stateless test
-- instead of an integration test - the relevant logic will still be covered by that.

SYSTEM DROP QUERY CACHE;

-- Run SELECT with quota that current user may write only 1 entry in the query cache
SET query_cache_max_entries = 1;
SELECT 'a' SETTINGS use_query_cache = true;
SELECT 'b' SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache; -- expect 1 entry

-- Run SELECTs again but w/o quota
SET query_cache_max_entries = DEFAULT;
SELECT 'c' SETTINGS use_query_cache = true;
SELECT 'd' SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache; -- expect 3 entries

SYSTEM DROP QUERY CACHE;

-- Run the same as above after a DROP QUERY CACHE.
SELECT '--';

SET query_cache_max_entries = 1;
SELECT 'a' SETTINGS use_query_cache = true;
SELECT 'b' SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache; -- expect 1 entry

-- Run SELECTs again but w/o quota
SET query_cache_max_entries = DEFAULT;
SELECT 'c' SETTINGS use_query_cache = true;
SELECT 'd' SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache; -- expect 3 entries

SYSTEM DROP QUERY CACHE;

-- SELECT '---';

