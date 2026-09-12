-- Tags: no-random-settings, no-random-merge-tree-settings
-- The legacy read-in-order entry point (the old analyzer with `query_plan_read_in_order = 0`)
-- sets `query_info.input_order_info` directly in `InterpreterSelectQuery` instead of going
-- through `ReadFromMergeTree::requestReadingInOrder`, so none of the opt-outs that stamp
-- `prefer_multiple_streams` (residual filter, `LIMIT BY`, aggregation-/distinct-in-order,
-- non-trivial `ORDER BY` expression) are applied on that path. `PrefetchingConcatProcessor`
-- must therefore stay disabled for it entirely; only the query-plan optimizer path may use it.

SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_legacy_order;

CREATE TABLE t_legacy_order (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_legacy_order FINAL;

-- Positive control: on the modern query-plan path this exact shape uses PrefetchingConcat,
-- so the assertions below cannot pass vacuously.
SELECT 'modern_prefetching_used';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_legacy_order
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- The legacy path must not use PrefetchingConcat, even for the plain shape the modern path allows:
-- the opt-out analysis never ran, so there is no way to know the downstream needs multiple streams.
SELECT 'no_prefetching_legacy_plain';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_legacy_order
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%'
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0;

-- Control against vacuity: the legacy path really is reading in order for that query
-- (no full-sort transform in the pipeline), so PrefetchingConcat was avoided by the gate,
-- not because read-in-order did not apply.
SELECT 'legacy_reads_in_order';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_legacy_order
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%MergeSortingTransform%'
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0;

-- The regression shape from the review: a residual `WHERE` above the read (kept out of `PREWHERE`)
-- is per-row CPU work that must stay parallel across streams. On the legacy path the residual-filter
-- opt-out never runs, so PrefetchingConcat would have collapsed the single-part read to one stream
-- and serialized the `FilterStep`.
SELECT 'no_prefetching_legacy_residual_filter';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE
    SELECT * FROM t_legacy_order
    PREWHERE path LIKE '%file.log'
    WHERE value % 7 = 0
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%'
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

-- Correctness of the legacy-path read: sorted output and the exact residual-filter row count.
SELECT 'legacy_residual_filter_correctness';
SELECT
    countIf(path < prev_path) = 0 AS is_sorted,
    count() = (SELECT countIf(value % 7 = 0) FROM t_legacy_order WHERE path LIKE '%file.log') AS count_matches
FROM (
    SELECT path, lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (
        SELECT path FROM t_legacy_order
        PREWHERE path LIKE '%file.log'
        WHERE value % 7 = 0
        ORDER BY path
    )
) SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

-- The same hole reaches `ENGINE = Merge` children: `ReadFromMerge::createChildrenPlans` stamps the
-- children's `input_order_info` from `query_info.order_optimizer` on the legacy path, also without
-- `requestReadingInOrder`. Plain `EXPLAIN PIPELINE` attributes no processors to `ReadFromMerge`
-- (child pipelines are built separately), so use `graph = 1`, which dumps the full processor graph.
DROP TABLE IF EXISTS t_legacy_order_merge;
CREATE TABLE t_legacy_order_merge ENGINE = Merge(currentDatabase(), '^t_legacy_order$');

SELECT 'no_prefetching_legacy_merge_child';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1
    SELECT * FROM t_legacy_order_merge
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%'
SETTINGS enable_analyzer = 0, query_plan_read_in_order = 0;

DROP TABLE t_legacy_order_merge;
DROP TABLE t_legacy_order;
