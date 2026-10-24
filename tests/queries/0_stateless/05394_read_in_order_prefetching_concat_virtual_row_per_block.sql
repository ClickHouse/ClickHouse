-- Tags: no-random-settings, no-random-merge-tree-settings
-- With `read_in_order_use_virtual_row_per_block`, the sources emit virtual-row chunks whose
-- primary key payload is not counted by the read-ahead budget of `PrefetchingConcatProcessor`,
-- so a filtered single-part read in order must not use it.

SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;

DROP TABLE IF EXISTS t_prefetching_concat_vr;

CREATE TABLE t_prefetching_concat_vr (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_prefetching_concat_vr FINAL;

SELECT 'without_virtual_row_per_block';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_vr
    WHERE value % 1000 = 0
    ORDER BY path
    SETTINGS read_in_order_use_virtual_row_per_block = 0
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'with_virtual_row_per_block';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_vr
    WHERE value % 1000 = 0
    ORDER BY path
    SETTINGS read_in_order_use_virtual_row_per_block = 1
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'correctness';
SELECT count(), countIf(path < prev_path) AS violations FROM (
    SELECT path, lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (
        SELECT path FROM t_prefetching_concat_vr
        WHERE value % 1000 = 0
        ORDER BY path
        SETTINGS read_in_order_use_virtual_row_per_block = 1
    )
);

DROP TABLE t_prefetching_concat_vr;
