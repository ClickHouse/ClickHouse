-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests per-user quotas of the query cache. Settings 'query_cache_max_size_in_bytes' and 'query_cache_max_entries' are actually supposed to
-- be used in a settings profile, together with a readonly constraint. For simplicity, test both settings stand-alone in a stateless test
-- instead of an integration test - the relevant logic will still be covered by that.

SYSTEM DROP QUERY CACHE;

SET query_cache_max_size_in_bytes = 1;
SELECT 'Run SELECT with quota that current user may use only 1 byte in the query cache', 1 SETTINGS use_query_cache = true;
SELECT 'Expect no entries in the query cache', count(*) FROM system.query_cache;

SET query_cache_max_size_in_bytes = DEFAULT;
SELECT 'Run SELECT again but w/o quota', 1 SETTINGS use_query_cache = true;
SELECT 'Expect one entry in the query cache',  count(*) FROM system.query_cache;

SELECT '---';
SYSTEM DROP QUERY CACHE;

SELECT 'Run SELECT which writes its result in the query cache', 1 SETTINGS use_query_cache = true;
SET query_cache_max_entries = 1;
SELECT 'Run another SELECT with quota that current user may write only 1 entry in the query cache', 1 SETTINGS use_query_cache = true;
SELECT 'Expect one entry in the query cache', count(*) FROM system.query_cache;
SET query_cache_max_entries = DEFAULT;
SELECT 'Run another SELECT w/o quota', 1 SETTINGS use_query_cache = true;
SELECT 'Expect two entries in the query cache', count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
