-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;
DROP TABLE IF EXISTS t;

-- Create test table with lot's of rows
CREATE TABLE t(c String) ENGINE=MergeTree ORDER BY c;
INSERT INTO t SELECT multiIf(n = 0, 'abc', n = 1, 'def', n = 2, 'abc', n = 3, 'jkl', '<unused>') FROM (SELECT number % 4 AS n FROM numbers(1200));
OPTIMIZE TABLE t FINAL;

-- Run query which, store *compressed* result in query cache
SELECT '-- insert with enabled compression';
SELECT * FROM t ORDER BY c
SETTINGS use_query_cache = true, query_cache_compress_entries = true;

-- Run again to check that no bad things happen and that the result is as expected
SELECT '-- read from cache';
SELECT * FROM t ORDER BY c
SETTINGS use_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- Run query which, store *uncompressed* result in query cache
SELECT '-- insert with disabled compression';
SELECT * FROM t ORDER BY c
SETTINGS use_query_cache = true, query_cache_compress_entries = false;

-- Run again to check that no bad things happen and that the result is as expected
SELECT '-- read from cache';
SELECT * FROM t ORDER BY c
SETTINGS use_query_cache = true;

DROP TABLE t;
SYSTEM DROP QUERY CACHE;
