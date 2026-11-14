-- Tags: no-random-settings, no-random-merge-tree-settings
-- `read_in_order_use_buffering = 0` disables `PrefetchingConcatProcessor` for read-in-order from a single part.

SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_prefetching_concat_buffering;

CREATE TABLE t_prefetching_concat_buffering (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_prefetching_concat_buffering FINAL;

SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_buffering WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS read_in_order_use_buffering = 1
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_buffering WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS read_in_order_use_buffering = 0
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT count(), countIf(path < prev_path) FROM (
    SELECT path, lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (SELECT path FROM t_prefetching_concat_buffering WHERE path LIKE '%file.log' ORDER BY path SETTINGS read_in_order_use_buffering = 0)
);

DROP TABLE t_prefetching_concat_buffering;
