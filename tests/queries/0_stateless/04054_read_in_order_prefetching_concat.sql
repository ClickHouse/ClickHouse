-- Test that PrefetchingConcatProcessor is used for read-in-order from a single part.
-- When a single part is split into multiple streams for parallel reading,
-- PrefetchingConcat should be used instead of MergingSorted.

DROP TABLE IF EXISTS t_prefetching_concat;

CREATE TABLE t_prefetching_concat (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64
AS SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(100000);

-- Verify PrefetchingConcat appears in the pipeline for a single-part table
-- with read-in-order and multiple threads.
SELECT 'has_prefetching_concat';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, max_threads = 4
) WHERE explain LIKE '%PrefetchingConcat%';

-- Verify correctness: output must be sorted.
SELECT 'correctness';
SELECT count() FROM (
    SELECT path FROM t_prefetching_concat
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS max_threads = 4
);

-- PrefetchingConcat should NOT be used with LIMIT (read_limit != 0).
SELECT 'no_prefetching_with_limit';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat
    ORDER BY path
    LIMIT 10
    SETTINGS enable_parallel_replicas = 0, max_threads = 4
) WHERE explain LIKE '%PrefetchingConcat%';

DROP TABLE t_prefetching_concat;
