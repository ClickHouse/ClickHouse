-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Regression test: a non-stale disk entry must not block repopulation of the memory cache when the query is
-- not allowed to read from disk (enable_reads_from_query_cache_disk = 0). Previously the writer admission check
-- treated a fresh disk entry as "not stale" and skipped the memory insert entirely.

SYSTEM DROP QUERY CACHE;
DROP TABLE IF EXISTS t;

CREATE TABLE t(id Int64, c String) ENGINE=MergeTree ORDER BY id;
INSERT INTO t SELECT number, concat('abc_', number) FROM numbers(10);

SELECT '-- populate memory and disk';
SELECT * FROM t ORDER BY id
SETTINGS use_query_cache = true, enable_writes_to_query_cache_disk = true, enable_reads_from_query_cache_disk = true;

SELECT '-- drop memory cache, only the disk entry remains';
SYSTEM DROP QUERY CACHE TYPE 'Memory';
SELECT count() FROM system.query_cache WHERE type = 'Memory';
SELECT count() FROM system.query_cache WHERE type = 'Disk';

SELECT '-- run again with disk reads disabled: must repopulate the memory cache';
SELECT * FROM t ORDER BY id
SETTINGS use_query_cache = true, enable_writes_to_query_cache_disk = false, enable_reads_from_query_cache_disk = false;

SELECT '-- memory cache is now populated again';
SELECT count() FROM system.query_cache WHERE type = 'Memory';

DROP TABLE t;
SYSTEM DROP QUERY CACHE;
