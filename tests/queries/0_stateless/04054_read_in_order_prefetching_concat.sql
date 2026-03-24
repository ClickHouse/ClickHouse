-- Tags: no-random-settings, no-random-merge-tree-settings

-- Test that PrefetchingConcatProcessor is used for read-in-order from a single part.
-- When a single part is split into multiple streams for parallel reading,
-- PrefetchingConcat should be used instead of MergingSorted.

DROP TABLE IF EXISTS t_prefetching_concat;

CREATE TABLE t_prefetching_concat (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_prefetching_concat FINAL;

-- Verify PrefetchingConcat appears in the pipeline for a single-part table
-- with read-in-order and multiple threads.
SELECT 'has_prefetching_concat';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1
) WHERE explain LIKE '%PrefetchingConcat%';

-- Verify correctness: output must be sorted.
SELECT 'correctness';
SELECT countIf(path < prev_path) FROM (
    SELECT path, lagInFrame(path, 1, '') OVER (ORDER BY path) AS prev_path
    FROM t_prefetching_concat WHERE path LIKE '%file.log' ORDER BY path SETTINGS max_threads = 4, optimize_read_in_order = 1
) WHERE prev_path != '';

-- PrefetchingConcat should NOT be used with LIMIT (read_limit != 0).
SELECT 'no_prefetching_with_limit';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat
    ORDER BY path
    LIMIT 10
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1
) WHERE explain LIKE '%PrefetchingConcat%';

DROP TABLE t_prefetching_concat;

-- PrefetchingConcat should be used per-part for multi-part tables (with partitions).
-- Each part gets parallel streams with PrefetchingConcat, MergingSorted merges between parts.
DROP TABLE IF EXISTS t_prefetching_concat_multi;
CREATE TABLE t_prefetching_concat_multi (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 1000000) ORDER BY key;
INSERT INTO t_prefetching_concat_multi SELECT number, toString(number) FROM numbers(3000000);
OPTIMIZE TABLE t_prefetching_concat_multi FINAL;

SELECT 'prefetching_multi_part';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_multi
    WHERE value LIKE '%5%'
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1
) WHERE explain LIKE '%PrefetchingConcat%';

-- Correctness: output must be sorted across partitions.
SELECT 'multi_part_correctness';
SELECT countIf(key < prev_key) FROM (
    SELECT key, lagInFrame(key, 1, 0) OVER (ORDER BY key) AS prev_key
    FROM t_prefetching_concat_multi WHERE value LIKE '%5%' ORDER BY key SETTINGS max_threads = 6, optimize_read_in_order = 1
) WHERE prev_key != 0;

DROP TABLE t_prefetching_concat_multi;

-- PrefetchingConcat should also work for reverse-order reads (ORDER BY ... DESC).
DROP TABLE IF EXISTS t_prefetching_concat_reverse;
CREATE TABLE t_prefetching_concat_reverse (key UInt64, value String)
ENGINE = MergeTree ORDER BY key;
INSERT INTO t_prefetching_concat_reverse SELECT number, toString(number) FROM numbers(1000000);
OPTIMIZE TABLE t_prefetching_concat_reverse FINAL;

SELECT 'reverse_has_prefetching';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_reverse
    WHERE value LIKE '%5%'
    ORDER BY key DESC
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1
) WHERE explain LIKE '%PrefetchingConcat%';

-- Correctness: output must be in descending order.
SELECT 'reverse_correctness';
SELECT countIf(key > prev_key) FROM (
    SELECT key, lagInFrame(key, 1, 0) OVER (ORDER BY key DESC) AS prev_key
    FROM t_prefetching_concat_reverse WHERE value LIKE '%5%' ORDER BY key DESC SETTINGS max_threads = 4, optimize_read_in_order = 1
) WHERE prev_key != 0;

DROP TABLE t_prefetching_concat_reverse;
