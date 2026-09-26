-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- A read task is handed out in several output blocks when its rows exceed `max_block_size`
-- or its bytes exceed `preferred_block_size_bytes`, which is the ordinary case: a task spans
-- at least `merge_tree_min_rows_for_concurrent_read` rows, several times the default block.
-- The cache stores one entry per contiguous mark range, so the first read has to carry the
-- rows of a range across all of its blocks to write the entry, and the repeated read has to
-- be served from the entry across all of its blocks - whatever its own block limits are.

SET max_threads = 1;
SET use_columns_cache = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS t_cc_multi_block;

CREATE TABLE t_cc_multi_block (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

-- A single part of 200000 rows: many more than a block, and `s` is 20 MB uncompressed,
-- many times `preferred_block_size_bytes`.
INSERT INTO t_cc_multi_block SELECT number, repeat('x', 100) FROM numbers(200000);

SYSTEM DROP COLUMNS CACHE;

-- The first read populates the cache. It really is split into several blocks (checked
-- through `blockNumber`), so the deferred write has to survive the continuation reads.
SELECT uniqExact(blockNumber()) > 1, sum(id), uniqExact(s) FROM t_cc_multi_block
SETTINGS max_block_size = 65536, preferred_block_size_bytes = 1000000;

-- Every read column of the part is cached as a whole. The part may be read as one task or as
-- several (remote disks size tasks differently), so the entries are checked for their coverage
-- of the part, not for their number.
SELECT column, min(row_begin), max(row_end), sum(rows)
FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cc_multi_block'
GROUP BY column
ORDER BY column;

-- The repeated read with the same block limits is served from the cache.
SELECT uniqExact(blockNumber()) > 1, sum(id), uniqExact(s) FROM t_cc_multi_block
SETTINGS max_block_size = 65536, preferred_block_size_bytes = 1000000;

-- So is a read of the same range with other block limits: the entries do not depend on
-- how the range was split into blocks when it was written.
SELECT uniqExact(blockNumber()) > 1, sum(id), uniqExact(s) FROM t_cc_multi_block
SETTINGS max_block_size = 10000, preferred_block_size_bytes = 0;

SYSTEM FLUSH LOGS query_log;

-- The first read misses and the two repeated reads hit, without a single miss.
SELECT
    ProfileEvents['ColumnsCacheHits'] > 0 AS has_hits,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%uniqExact(s) FROM t_cc_multi_block%'
    AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_cc_multi_block;

-- A member of a `Nested` that was added after the part was written has no data stream in the
-- part: only its offsets are read, from a sibling, and its elements stay empty. Such a column is
-- never cached, and its rows must not be copied for the cache either, whichever block of the
-- range they come out in. The siblings that are fully present in the part are cached as usual.
DROP TABLE IF EXISTS t_cc_nested_added;

CREATE TABLE t_cc_nested_added (id UInt64, n Nested(a UInt64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

INSERT INTO t_cc_nested_added SELECT number, [number, number + 1], ['x', 'y'] FROM numbers(200000);

ALTER TABLE t_cc_nested_added ADD COLUMN n.c Array(Float64);

SYSTEM DROP COLUMNS CACHE;

SELECT sum(arraySum(n.a)), sum(length(arrayStringConcat(n.b))), countIf(n.c != [0., 0.]) FROM t_cc_nested_added
SETTINGS max_block_size = 65536, preferred_block_size_bytes = 1000000;

SELECT sum(arraySum(n.a)), sum(length(arrayStringConcat(n.b))), countIf(n.c != [0., 0.]) FROM t_cc_nested_added
SETTINGS max_block_size = 65536, preferred_block_size_bytes = 1000000;

SELECT column, min(row_begin), max(row_end), sum(rows)
FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cc_nested_added'
GROUP BY column
ORDER BY column;

DROP TABLE t_cc_nested_added;
