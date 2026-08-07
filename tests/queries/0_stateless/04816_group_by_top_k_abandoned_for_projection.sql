-- Aggregate-projection rewriting runs after the top-K optimization and turns the
-- aggregation into merge-only (all parts have the projection) or into an
-- `AggregatingProjection` step (only some parts do).  Merging of aggregation states
-- bypasses the heap, so the rewrite must abandon `top_k`: otherwise the plan keeps
-- a stale `Top-K` annotation and, for the no-`ORDER BY` shape, the sort synthesized
-- for the heap, making the projection plan slower than the non-optimized one.

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
-- The trivial `GROUP BY ... LIMIT` rewrite sets max_rows_to_group_by, which
-- disables the top-K optimization for aggregate-free projections; keep it off.
SET optimize_trivial_group_by_limit_query = 0;
SET enable_analyzer = 1;
SET optimize_use_projections = 1;
-- This test observes single-node plan shapes.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_top_k_proj;

CREATE TABLE t_top_k_proj
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT k, sum(v) GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_top_k_proj SELECT number % 5000, number FROM numbers(100000);

-- Sanity check: with projections off, the optimization applies and the plan
-- carries the Top-K annotation and the sort synthesized for it.
SELECT 'projections_off';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_proj GROUP BY k LIMIT 10
    SETTINGS optimize_use_projections = 0
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Top-K%' OR explain LIKE '%AggregatingProjection%';

-- All parts have the projection: the aggregation becomes merge-only, the heap
-- cannot run, so no Top-K annotation and no leftover synthesized sort.
SELECT 'full_projection';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_proj GROUP BY k LIMIT 10
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Top-K%' OR explain LIKE '%AggregatingProjection%';

-- The `ORDER BY` shape keeps its real sort but must drop the Top-K annotation.
SELECT 'full_projection_order_by';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_proj GROUP BY k ORDER BY k LIMIT 10
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Top-K%' OR explain LIKE '%AggregatingProjection%';

-- Results are unaffected.
SELECT 'results_match';
SELECT count() FROM
(
    SELECT k, sum(v) AS s FROM t_top_k_proj GROUP BY k ORDER BY k LIMIT 10
) AS o
FULL JOIN
(
    SELECT k, sum(v) AS s FROM t_top_k_proj GROUP BY k ORDER BY k LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 0
) AS u USING (k)
WHERE o.s != u.s;

DROP TABLE t_top_k_proj;

-- Mixed path: one part without the projection and one with it produces an
-- `AggregatingProjection` step; the heap must be abandoned there too.
DROP TABLE IF EXISTS t_top_k_proj_mixed;

CREATE TABLE t_top_k_proj_mixed (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_top_k_proj_mixed SELECT number % 5000, number FROM numbers(50000);

ALTER TABLE t_top_k_proj_mixed ADD PROJECTION p (SELECT k, sum(v) GROUP BY k);

INSERT INTO t_top_k_proj_mixed SELECT number % 5000, number FROM numbers(50000, 50000);

SELECT 'mixed_parts';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_proj_mixed GROUP BY k LIMIT 10
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Top-K%' OR explain LIKE '%AggregatingProjection%';

SELECT 'mixed_results_match';
SELECT count() FROM
(
    SELECT k, sum(v) AS s FROM t_top_k_proj_mixed GROUP BY k ORDER BY k LIMIT 10
) AS o
FULL JOIN
(
    SELECT k, sum(v) AS s FROM t_top_k_proj_mixed GROUP BY k ORDER BY k LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 0
) AS u USING (k)
WHERE o.s != u.s;

DROP TABLE t_top_k_proj_mixed;
