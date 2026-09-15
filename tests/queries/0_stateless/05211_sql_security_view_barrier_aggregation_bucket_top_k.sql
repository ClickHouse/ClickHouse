-- The bucket top-K plan optimization (`query_plan_aggregation_bucket_top_k`) lets an outer
-- `ORDER BY count() LIMIT n` retune the aggregation below it: the final conversion keeps only
-- each two-level bucket's best n groups. It must not cross the seal of a view with
-- `SQL SECURITY DEFINER` or `SQL SECURITY NONE`: the invoker's `LIMIT` must not retune the
-- processing of the view's own aggregation. See IQueryPlanStep::isSecurityBarrier.

SET enable_parallel_replicas = 0;
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;
-- The sibling GROUP BY top-K rewrite would change the plan shape below the LIMIT; this test is about the bucket rule alone.
SET enable_group_by_top_k_optimization = 0;
-- Randomized by the test harness; the rule needs a final, not in-order, aggregation for the control.
SET optimize_aggregation_in_order = 0;
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_abtk;
CREATE TABLE t_abtk (k UInt32, val UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_abtk SELECT number % 100, number FROM numbers(10000);

CREATE VIEW v_abtk_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, count() AS c FROM t_abtk WHERE k != 42 GROUP BY k;
CREATE VIEW v_abtk_none SQL SECURITY NONE AS SELECT k, count() AS c FROM t_abtk WHERE k != 42 GROUP BY k;
CREATE VIEW v_abtk_invoker SQL SECURITY INVOKER AS SELECT k, count() AS c FROM t_abtk WHERE k != 42 GROUP BY k;

-- Only the analyzer path: the legacy analyzer wraps a view's outputs in `materialize`, so the
-- sort column never traces to the aggregation's `count()` and the rule does not fire even for the invoker.
SET enable_analyzer = 1;
SELECT 'definer bucket top-K markers (expect 0):', countIf(explain LIKE '%Bucket top-K:%')
FROM (EXPLAIN actions = 1 SELECT k, c FROM v_abtk_definer ORDER BY c DESC LIMIT 5);
SELECT 'none bucket top-K markers (expect 0):', countIf(explain LIKE '%Bucket top-K:%')
FROM (EXPLAIN actions = 1 SELECT k, c FROM v_abtk_none ORDER BY c DESC LIMIT 5);
SELECT 'invoker bucket top-K markers (expect 1):', countIf(explain LIKE '%Bucket top-K:%')
FROM (EXPLAIN actions = 1 SELECT k, c FROM v_abtk_invoker ORDER BY c DESC LIMIT 5);

DROP VIEW v_abtk_definer;
DROP VIEW v_abtk_none;
DROP VIEW v_abtk_invoker;
DROP TABLE t_abtk;
