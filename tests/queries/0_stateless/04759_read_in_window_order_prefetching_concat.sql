-- Tags: no-random-settings, no-random-merge-tree-settings
-- The legacy window-order reuse path (`query_plan_reuse_storage_ordering_for_window_functions`,
-- old analyzer only) allows `WindowStep <- SortingStep <- [Expression] <- ReadFromMergeTree`.
-- With an explicit `PREWHERE` (a plan-level `WHERE` becomes a `FilterStep` and never matches
-- this shape), the `ExpressionStep` between the read and the sort materializes the window
-- `PARTITION BY` / `ORDER BY` keys per stream, in parallel. `PrefetchingConcatProcessor`
-- must NOT collapse a single-part filtered read into one stream there - that would
-- serialize the per-stream expression work that used to run below the sort.

SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET enable_analyzer = 0;
SET optimize_read_in_order = 0;
SET optimize_read_in_window_order = 1;
SET query_plan_reuse_storage_ordering_for_window_functions = 1;

DROP TABLE IF EXISTS t_window_prefetching_concat;

-- Unique `path` values, so the sliding-window sum ordered by `path` is deterministic.
CREATE TABLE t_window_prefetching_concat (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_window_prefetching_concat FINAL;

-- The reused storage ordering must still kick in: the read is in order.
SELECT 'window_order_reused';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT path, sum(value) OVER (ORDER BY path ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS w
    FROM t_window_prefetching_concat
    PREWHERE path LIKE '%file.log'
) WHERE explain LIKE '%InOrder%';

-- PrefetchingConcat must NOT appear: the ExpressionStep below the sort runs per stream.
SELECT 'no_prefetching_window_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT path, sum(value) OVER (ORDER BY path ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS w
    FROM t_window_prefetching_concat
    PREWHERE path LIKE '%file.log'
) WHERE explain LIKE '%PrefetchingConcat%';

-- Correctness: the window result over the reused storage ordering must match the same
-- query computed single-threaded with a full sort (no storage-ordering reuse).
SELECT 'window_correctness';
SELECT
    (SELECT sum(cityHash64(path, w)) FROM (
        SELECT path, sum(value) OVER (ORDER BY path ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS w
        FROM t_window_prefetching_concat
        PREWHERE path LIKE '%file.log'
    ))
    ==
    (SELECT sum(cityHash64(path, w)) FROM (
        SELECT path, sum(value) OVER (ORDER BY path ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS w
        FROM t_window_prefetching_concat
        PREWHERE path LIKE '%file.log'
        SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0, max_threads = 1
    ));

DROP TABLE t_window_prefetching_concat;
