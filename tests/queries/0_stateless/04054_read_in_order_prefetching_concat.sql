-- Tags: no-random-settings, no-random-merge-tree-settings
-- Test that PrefetchingConcatProcessor is used for read-in-order from a single part.
-- When a single part is split into multiple streams for parallel reading,
-- PrefetchingConcat should be used instead of MergingSorted.

-- The presence of PrefetchingConcat in the pipeline depends on many
-- `MergeTree` and query-plan settings (e.g. `read_in_order_use_virtual_row`,
-- `query_plan_optimize_lazy_materialization`, `index_granularity_bytes`,
-- `min_bytes_for_wide_part`) that interact in non-trivial ways with the
-- splitting/prewhere paths. Rather than enumerate every relevant flag,
-- disable randomization entirely so the test is deterministic.
SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_prefetching_concat;

CREATE TABLE t_prefetching_concat (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_prefetching_concat FINAL;

-- Verify PrefetchingConcat appears in the pipeline for a single-part table
-- with read-in-order and multiple threads.
SELECT 'has_prefetching_concat';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- Verify correctness: output must be sorted.
-- Check that no adjacent pair violates ordering using lagInFrame() window function.
SELECT 'correctness';
SELECT count(), countIf(path < prev_path) AS violations FROM (
    SELECT path, lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (
        SELECT path FROM t_prefetching_concat
        WHERE path LIKE '%file.log'
        ORDER BY path
    )
);

-- Distinct-in-order runs a parallel pre-distinct transform per stream. PrefetchingConcat
-- must NOT collapse those streams into one (it would serialize the deduplication), even when
-- the read order is applied from plan sorting properties (the `applyOrder` path) rather than by
-- `optimizeDistinctInOrder`. Here the prefix for ORDER BY and DISTINCT is the same, so the order
-- is satisfied via `applyOrder`. Expect no PrefetchingConcat, parallel pre-distinct, sorted output.
SELECT 'no_prefetching_distinct_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT DISTINCT path FROM t_prefetching_concat
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%'
SETTINGS optimize_distinct_in_order = 1;

SELECT 'distinct_correctness';
SELECT arr = arraySort(arr) AS is_sorted, length(arr) AS n FROM (
    SELECT groupArray(path) AS arr FROM (
        SELECT DISTINCT path FROM t_prefetching_concat
        WHERE path LIKE '%file.log'
        ORDER BY path
    )
) SETTINGS optimize_distinct_in_order = 1;

DROP TABLE t_prefetching_concat;

-- PrefetchingConcat should NOT be used with multiple parts whose ranges
-- are in reverse order after the split (the splitting takes parts from the back).
DROP TABLE IF EXISTS t_prefetching_concat_multi;
CREATE TABLE t_prefetching_concat_multi (key UInt64, value String)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192;
SYSTEM STOP MERGES t_prefetching_concat_multi;
INSERT INTO t_prefetching_concat_multi SELECT number, toString(number) FROM numbers(100000);
INSERT INTO t_prefetching_concat_multi SELECT number + 100000, toString(number) FROM numbers(100000);
INSERT INTO t_prefetching_concat_multi SELECT number + 200000, toString(number) FROM numbers(100000);

SELECT 'no_prefetching_multi_part';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_multi
    WHERE value LIKE '%5%'
    ORDER BY key
) WHERE explain LIKE '%PrefetchingConcat%';

DROP TABLE t_prefetching_concat_multi;
