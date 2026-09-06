-- The GROUP BY top-K optimization turns an aggregation below a LIMIT into a bounded heap and may
-- synthesize a sorting step below the LIMIT. It must not cross the seal of a view with
-- `SQL SECURITY DEFINER` or `SQL SECURITY NONE`: the invoker's LIMIT must not retune the
-- processing of the view's own aggregation. See IQueryPlanStep::isSecurityBarrier.

SET enable_parallel_replicas = 0;
SET enable_group_by_top_k_optimization = 1;
-- Randomized by the test harness; a value below the LIMIT would disable the optimization for the control.
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- The optimization is disabled under plan serialization.
SET serialize_query_plan = 0;
-- Randomized by the test harness; an in-order aggregation is not eligible for the top-K heap,
-- which would disable the optimization for the control.
SET optimize_aggregation_in_order = 0;
-- The stateless test profile sets `max_rows_to_group_by` to a huge-but-non-zero value, which
-- makes the aggregation ineligible for the top-K heap.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_gbtk;
CREATE TABLE t_gbtk (k UInt32, val UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_gbtk SELECT number % 100, number FROM numbers(10000);

CREATE VIEW v_gbtk_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, sum(val) AS s FROM t_gbtk WHERE k != 42 GROUP BY k;
CREATE VIEW v_gbtk_invoker SQL SECURITY INVOKER AS SELECT k, sum(val) AS s FROM t_gbtk WHERE k != 42 GROUP BY k;

SELECT 'analyzer:';
SET enable_analyzer = 1;
SELECT 'definer top-K markers (expect 0):', countIf(explain LIKE '%Top-K:%' OR explain LIKE '%Sorting for GROUP BY top-K%')
FROM (EXPLAIN actions = 1 SELECT k, s FROM v_gbtk_definer LIMIT 5);
SELECT 'invoker top-K markers (expect 2):', countIf(explain LIKE '%Top-K:%' OR explain LIKE '%Sorting for GROUP BY top-K%')
FROM (EXPLAIN actions = 1 SELECT k, s FROM v_gbtk_invoker LIMIT 5);

SELECT 'legacy analyzer:';
SET enable_analyzer = 0;
SELECT 'definer top-K markers (expect 0):', countIf(explain LIKE '%Top-K:%' OR explain LIKE '%Sorting for GROUP BY top-K%')
FROM (EXPLAIN actions = 1 SELECT k, s FROM v_gbtk_definer LIMIT 5);
SELECT 'invoker top-K markers (expect 2):', countIf(explain LIKE '%Top-K:%' OR explain LIKE '%Sorting for GROUP BY top-K%')
FROM (EXPLAIN actions = 1 SELECT k, s FROM v_gbtk_invoker LIMIT 5);

DROP VIEW v_gbtk_definer;
DROP VIEW v_gbtk_invoker;
DROP TABLE t_gbtk;
