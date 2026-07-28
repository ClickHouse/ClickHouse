-- Tags: no-random-settings, no-random-merge-tree-settings, no-old-analyzer
-- The old analyzer does not apply the lazy-`FINAL` optimization, so the `FINAL` read keeps its
-- `ReplacingSorted` transforms and never becomes a plain read where `PrefetchingConcatProcessor`
-- is applicable - the baseline check below would see no `PrefetchingConcat` at all.
-- `optimizeReadInOrder` runs before `optimizeLazyFinal`, so a `prefer_multiple_streams` opt-out
-- recorded on the original `ReadFromMergeTree` must be carried over to the non-`FINAL` read that
-- `optimizeLazyFinal` synthesizes for non-intersecting parts. Otherwise the replacement read
-- re-enables `PrefetchingConcatProcessor` and collapses exactly the per-stream pipeline the
-- optimizer asked to keep parallel.
--
-- As in `04054_read_in_order_prefetching_concat`, the pipeline shape depends on many `MergeTree`
-- and query-plan settings, so randomization is disabled and the relevant ones are pinned.
SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;
SET query_plan_optimize_lazy_final = 1;

DROP TABLE IF EXISTS t_prefetching_concat_lazy_final;

-- A single part is trivially non-intersecting, so `optimizeLazyFinal` replaces the `FINAL` read
-- with a plain non-`FINAL` one, which is where `PrefetchingConcatProcessor` becomes applicable.
CREATE TABLE t_prefetching_concat_lazy_final (path String, value UInt64)
ENGINE = ReplacingMergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', leftPad(toString(number), 8, '0'), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_prefetching_concat_lazy_final FINAL;

-- Baseline: with nothing asking for parallel streams, the synthesized read does use
-- `PrefetchingConcat`, which is what makes the checks below meaningful.
SELECT 'has_prefetching_concat_final';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_lazy_final FINAL
    PREWHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- A residual `WHERE` above the read is per-row CPU work that must stay parallel.
SELECT 'no_prefetching_residual_filter_final';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%FilterTransform × 4%') AS parallel_filter
FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_lazy_final FINAL
    PREWHERE path LIKE '%file.log'
    WHERE value % 7 = 0
    ORDER BY path
);

SELECT 'residual_filter_final_correctness';
SELECT
    countIf(path < prev_path) = 0 AS is_sorted,
    count() = (SELECT countIf(value % 7 = 0) FROM t_prefetching_concat_lazy_final FINAL WHERE path LIKE '%file.log') AS count_matches
FROM (
    SELECT path, lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (
        SELECT path FROM t_prefetching_concat_lazy_final FINAL
        PREWHERE path LIKE '%file.log'
        WHERE value % 7 = 0
        ORDER BY path
    )
);

-- Aggregation-in-order runs `AggregatingInOrderTransform` per stream.
SELECT 'no_prefetching_aggregation_in_order_final';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%AggregatingInOrderTransform × 4%') AS parallel_aggregation
FROM (
    EXPLAIN PIPELINE SELECT path, count() FROM t_prefetching_concat_lazy_final FINAL
    PREWHERE path LIKE '%file.log'
    GROUP BY path
) SETTINGS optimize_aggregation_in_order = 1;

SELECT 'aggregation_in_order_final_correctness';
SELECT count() = 1000000 AS ok, sum(c) = 1000000 AS total_ok FROM (
    SELECT path, count() AS c FROM t_prefetching_concat_lazy_final FINAL
    PREWHERE path LIKE '%file.log'
    GROUP BY path
) SETTINGS optimize_aggregation_in_order = 1;

-- `LIMIT BY`-in-order runs the `LimitBySortedStreamTransform` pre-filter per stream.
SELECT 'no_prefetching_limit_by_in_order_final';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%LimitBySortedStreamTransform × 4%') AS parallel_limit_by
FROM (
    EXPLAIN PIPELINE SELECT path FROM t_prefetching_concat_lazy_final FINAL
    PREWHERE path LIKE '%file.log'
    LIMIT 1 BY path
) SETTINGS optimize_limit_by_in_order = 1;

SELECT 'limit_by_in_order_final_correctness';
SELECT count() = 1000000 AS ok FROM (
    SELECT path FROM t_prefetching_concat_lazy_final FINAL
    PREWHERE path LIKE '%file.log'
    LIMIT 1 BY path
) SETTINGS optimize_limit_by_in_order = 1;

DROP TABLE t_prefetching_concat_lazy_final;
