-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;
DROP TABLE IF EXISTS t;

-- Create test table with "many" rows
CREATE TABLE t(c String) ENGINE=MergeTree ORDER BY c;
SYSTEM STOP MERGES t; -- retain multiple parts to make the SELECT process multiple chunks
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');
INSERT INTO t values ('abc') ('def') ('ghi') ('jkl');

-- Run query which reads multiple chunks (small max_block_size), cache result in query cache, force squashing of partial results
SELECT '-- insert with enabled squashing';
SELECT * FROM t ORDER BY c
SETTINGS max_block_size = 3, use_query_cache = true, query_cache_squash_partial_results = true;

-- Run again to check that no bad things happen and that the result is as expected
SELECT '-- read from cache';
SELECT * FROM t ORDER BY c
SETTINGS max_block_size = 3, use_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- Run query which reads multiple chunks (small max_block_size), cache result in query cache, but **disable** squashing of partial results
SELECT '-- insert with disabled squashing';
SELECT * FROM t ORDER BY c
SETTINGS max_block_size = 3, use_query_cache = true, query_cache_squash_partial_results = false;

-- Run again to check that no bad things happen and that the result is as expected
SELECT '-- read from cache';
SELECT * FROM t ORDER BY c
SETTINGS max_block_size = 3, use_query_cache = true;

DROP TABLE t;
SYSTEM DROP QUERY CACHE;
