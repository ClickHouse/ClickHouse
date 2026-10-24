-- Tags: no-random-settings, no-random-merge-tree-settings
-- A monotonic `ORDER BY` expression (e.g. `ORDER BY toDate(d)` over a `DateTime` sorting key)
-- reuses the storage order through a non-trivial `ExpressionStep` that materializes the sort
-- key per row. That is residual per-row CPU work above the read, exactly like a residual
-- `FilterStep`: the multi-stream path runs it in parallel (`ExpressionTransform` per stream),
-- while `PrefetchingConcatProcessor` would collapse the read into a single stream and
-- serialize it. So a non-trivial `ExpressionStep` must opt out of `PrefetchingConcat`,
-- while a trivial projection (inputs and aliases only) must not.

SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_aggregation_in_order = 0;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_monotonic_expr_prefetching;

CREATE TABLE t_monotonic_expr_prefetching (d DateTime, value UInt64)
ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 8192
AS SELECT toDateTime('2024-01-01 00:00:00') + number, number FROM numbers(1000000);

OPTIMIZE TABLE t_monotonic_expr_prefetching FINAL;

-- Positive control against vacuity: with a trivial projection (plain `ORDER BY d`),
-- the same single-part filtered read does use PrefetchingConcat.
SELECT 'baseline_prefetching_used';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_monotonic_expr_prefetching
    PREWHERE value % 10 != 9
    ORDER BY d
) WHERE explain LIKE '%PrefetchingConcat%';

-- The monotonic-expression match must still read in order.
SELECT 'monotonic_expression_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_monotonic_expr_prefetching
    PREWHERE value % 10 != 9
    ORDER BY toDate(d)
) WHERE explain LIKE '%InOrder%';

-- PrefetchingConcat must NOT appear: the `ExpressionStep` computing `toDate(d)` is residual
-- per-row CPU work, and the per-stream `ExpressionTransform`s must stay parallel.
SELECT 'no_prefetching_monotonic_expression';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_monotonic_expr_prefetching
    PREWHERE value % 10 != 9
    ORDER BY toDate(d)
) WHERE explain LIKE '%PrefetchingConcat%';

-- The expression work stays per-stream: the pipeline keeps a parallel ExpressionTransform stage.
SELECT 'monotonic_expression_parallel';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_monotonic_expr_prefetching
    PREWHERE value % 10 != 9
    ORDER BY toDate(d)
) WHERE explain LIKE '%ExpressionTransform × 4%';

-- Correctness: `toDate(d)` is non-unique (rows within a day may come in any order between
-- equal keys), so instead of an order-sensitive checksum verify that the output is sorted by
-- the expression, keeps exactly the filtered rows, and carries the same multiset of rows.
SELECT 'monotonic_expression_correctness';
SELECT
    countIf(k < prev_k) = 0 AS is_sorted,
    count() = (SELECT countIf(value % 10 != 9) FROM t_monotonic_expr_prefetching) AS count_matches,
    sum(row_hash) = (SELECT sum(cityHash64(d, value)) FROM t_monotonic_expr_prefetching WHERE value % 10 != 9) AS same_rows
FROM (
    SELECT k, cityHash64(d, value) AS row_hash, lagInFrame(k, 1, toDate('1970-01-01')) OVER (ORDER BY rowNumberInAllBlocks()) AS prev_k
    FROM (
        SELECT d, value, toDate(d) AS k FROM t_monotonic_expr_prefetching
        PREWHERE value % 10 != 9
        ORDER BY toDate(d)
    )
);

-- The same hole must be closed for a `Merge` table: the `ExpressionStep` computing the
-- monotonic sort key sits above `ReadFromMerge`, and the opt-out is forwarded to the child
-- reads. (`EXPLAIN PIPELINE graph = 1` is needed to see the child pipelines.)
DROP TABLE IF EXISTS merge_monotonic_expr_prefetching;
CREATE TABLE merge_monotonic_expr_prefetching (d DateTime, value UInt64)
ENGINE = Merge(currentDatabase(), '^t_monotonic_expr_prefetching$');

-- Positive control: the trivial-projection read through the `Merge` table still uses
-- PrefetchingConcat.
SELECT 'merge_baseline_prefetching_used';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT * FROM merge_monotonic_expr_prefetching
    PREWHERE value % 10 != 9
    ORDER BY d
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'merge_no_prefetching_monotonic_expression';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT * FROM merge_monotonic_expr_prefetching
    PREWHERE value % 10 != 9
    ORDER BY toDate(d)
) WHERE explain LIKE '%PrefetchingConcat%';

DROP TABLE merge_monotonic_expr_prefetching;
DROP TABLE t_monotonic_expr_prefetching;
