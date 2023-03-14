-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- Run query and store result in query cache but without compression which is on by default
SELECT 1 SETTINGS use_query_cache = true, query_cache_compress_entries = false;

-- Run again to check that no bad things happen and that the result is as expected
SELECT 1 SETTINGS use_query_cache = true;
