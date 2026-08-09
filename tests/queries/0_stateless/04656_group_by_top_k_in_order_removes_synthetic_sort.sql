-- For `GROUP BY ... LIMIT` without `ORDER BY` the top-K optimization synthesizes
-- a `SortingStep` that exists only to make the heap's pruning valid.  When
-- in-order aggregation later supersedes the heap it drops `top_k`, and that
-- synthesized sort has to go with it: leaving it behind blocks the LIMIT's early
-- termination, making the plan slower than the one without the optimization.

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
-- The trivial `GROUP BY ... LIMIT` rewrite sets max_rows_to_group_by, which
-- disables the top-K optimization for aggregate-free projections; keep it off.
SET optimize_trivial_group_by_limit_query = 0;
SET enable_analyzer = 1;
-- This test observes single-node plan shapes; under parallel replicas the
-- no-`ORDER BY` shape is gated off (the synthesized sort cannot sit above the
-- initiator's merge), so the `Top-K` annotation would never appear.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_top_k_in_order;

CREATE TABLE t_top_k_in_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_top_k_in_order SELECT number % 5000, number FROM numbers(200000);

-- With in-order aggregation off, the optimization applies: the plan carries the
-- Top-K annotation and the sort synthesized for it.
SELECT 'in_order_off';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_in_order GROUP BY k LIMIT 10
    SETTINGS optimize_aggregation_in_order = 0
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Top-K%';

-- With in-order aggregation on, the heap is superseded: no Top-K annotation and,
-- crucially, no leftover sort.  Before the sort was removed alongside the heap
-- this printed a `Sorting (Sorting for GROUP BY top-K)` line with no owner.
SELECT 'in_order_on';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_in_order GROUP BY k LIMIT 10
    SETTINGS optimize_aggregation_in_order = 1
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Top-K%';

-- The in-order plan must be the same shape the query gets without the
-- optimization at all.
DROP TABLE IF EXISTS gt_in_order_plan;
CREATE TABLE gt_in_order_plan (n UInt64, without_top_k String) ENGINE = Memory;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_in_order_plan
SELECT rowNumberInAllBlocks() AS n, explain FROM
(
    EXPLAIN SELECT k, sum(v) FROM t_top_k_in_order GROUP BY k LIMIT 10
    SETTINGS optimize_aggregation_in_order = 1
);
SET enable_group_by_top_k_optimization = 1;

SELECT 'in_order_on_matches_optimization_off';
SELECT countIf(with_top_k != without_top_k) FROM
(
    SELECT rowNumberInAllBlocks() AS n, explain AS with_top_k FROM
    (
        EXPLAIN SELECT k, sum(v) FROM t_top_k_in_order GROUP BY k LIMIT 10
        SETTINGS optimize_aggregation_in_order = 1
    )
) AS a
FULL JOIN gt_in_order_plan AS b USING (n);

-- Results are unaffected.  Without an ORDER BY the LIMIT picks an arbitrary 10
-- groups (the in-order pipeline re-shards the final merge across threads, so
-- even two identical statements can emit different groups); the runs cannot be
-- compared row-for-row.  Instead, every returned group must carry its complete
-- aggregate: 10 groups, each matching a full aggregation without the LIMIT.
SELECT 'results_match';
SELECT count(), countIf(o.s != truth.s) FROM
(
    SELECT k, sum(v) AS s FROM t_top_k_in_order GROUP BY k LIMIT 10
    SETTINGS optimize_aggregation_in_order = 1
) AS o
LEFT JOIN
(
    SELECT k, sum(v) AS s FROM t_top_k_in_order GROUP BY k
) AS truth USING (k);

DROP TABLE t_top_k_in_order;
DROP TABLE gt_in_order_plan;
