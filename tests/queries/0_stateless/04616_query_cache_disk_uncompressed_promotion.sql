-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Regression test: promoting an *uncompressed* (query_cache_compress_entries = 0) disk entry into the memory
-- cache must cache a clone, not the same Entry that is handed to the reader. Previously the uncompressed
-- promotion path stored the very shared pointer that the reader then moved its chunks/totals/extremes out of,
-- leaving the promoted memory entry empty; the next (memory) hit for the same query then returned an empty
-- result.

SYSTEM DROP QUERY CACHE;
DROP TABLE IF EXISTS t;

CREATE TABLE t(id Int64, c String) ENGINE=MergeTree ORDER BY id;
INSERT INTO t SELECT number, concat('abc_', number) FROM numbers(10);

SELECT '-- populate memory and disk with uncompressed entries';
SELECT count() FROM t
SETTINGS use_query_cache = true, query_cache_compress_entries = 0, enable_writes_to_query_cache_disk = true, enable_reads_from_query_cache_disk = true;

SELECT '-- drop memory cache, only the disk entry remains';
SYSTEM DROP QUERY CACHE TYPE 'Memory';
SELECT count() FROM system.query_cache WHERE type = 'Memory';
SELECT count() FROM system.query_cache WHERE type = 'Disk';

SELECT '-- disk hit promotes into the memory cache and serves the correct result';
SELECT count() FROM t
SETTINGS use_query_cache = true, query_cache_compress_entries = 0, enable_writes_to_query_cache_disk = true, enable_reads_from_query_cache_disk = true;

SELECT '-- memory cache is populated again';
SELECT count() FROM system.query_cache WHERE type = 'Memory';

SELECT '-- memory hit must return the same, non-empty result (10), not an empty promoted entry';
SELECT count() FROM t
SETTINGS use_query_cache = true, query_cache_compress_entries = 0, enable_writes_to_query_cache_disk = false, enable_reads_from_query_cache_disk = false;

DROP TABLE t;
SYSTEM DROP QUERY CACHE;
