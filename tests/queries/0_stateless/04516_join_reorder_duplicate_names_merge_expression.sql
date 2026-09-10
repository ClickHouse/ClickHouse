-- Regression test for a LOGICAL_ERROR ("Left and right columns have same names")
-- in the join order optimizer (`chooseJoinOrder` -> `JoinExpressionActions`). The optimizer
-- used to flatten a comma join over a multi-table view into the enclosing join graph, and with
-- `query_plan_merge_expression_into_join = 1` the non-passthrough expression step above the
-- view's inner join was merged into it, exposing one more relation than the pre-reorder
-- overlap guard accounted for. That extra relation duplicated a qualified column name
-- (`__table3.c1`), so reconstructing the reordered join hit a LOGICAL_ERROR. A view is now kept
-- whole, so that shape cannot arise from a view at all. The overlap guard still mirrors the
-- builder's flattening and skips reordering when names overlap, which is what covers the
-- non-view relations that can also collide.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=3d31d8f59df88ee56b9b739f2eedb1b7a6acc6a4&name_0=NightlySQLancer&name_1=SQLancerPP

DROP TABLE IF EXISTS t0_04516;
DROP TABLE IF EXISTS t4_04516;
DROP VIEW IF EXISTS v0_04516;

CREATE TABLE t0_04516 (c0 Bool, c1 Int32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t4_04516 (c0 Int32, c1 Bool) ENGINE = MergeTree ORDER BY c0;

-- Multi-table view: itself a comma join of t4 and t0 with a non-passthrough projection.
-- The projection name must not be a column name of either inner relation, because a projection
-- named after one of its own inputs is refused the merge that this test depends on.
CREATE VIEW v0_04516 AS
    SELECT concat(CAST(CASE t4_04516.c0 WHEN t0_04516.c1 THEN 'a' ELSE 'b' END AS String), 'x') AS d0
    FROM t4_04516, t0_04516
    HAVING (NOT CAST((NOT t4_04516.c0) AS Bool));

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_limit = 16;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_merge_expression_into_join = 1;

-- The point is that the query must not fail with a LOGICAL_ERROR during plan optimization.
SELECT t4_04516.c1, t0_04516.c1, v0_04516.d0, t0_04516.c0
FROM t0_04516, v0_04516, t4_04516;

-- A view's relations are not flattened into the enclosing join graph: a view restarts the `__tableN`
-- namespace, and two relations of one graph may not share a column name. The outer relation is
-- labelled by its alias and a flattened inner one by its database, so the two never end up in one
-- join label. The label also carries a cost annotation whose text depends on the randomized cost
-- model, hence the pin.
SELECT count() FROM (
    EXPLAIN SELECT t4_04516.c1, t0_04516.c1, v0_04516.d0, t0_04516.c0
    FROM t0_04516, v0_04516, t4_04516
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_merge_expression_into_join = 1, query_plan_optimize_join_order_randomize = 0
) WHERE explain LIKE '%t0_04516 × ' || currentDatabase() || '.t4_04516%';

-- The enclosing join is still reordered around the opaque view; `05039_join_reorder_view_barrier`
-- asserts that on a populated fixture, where the optimizer annotates the costed relations.

INSERT INTO t0_04516 VALUES (true, 1), (false, 2);
INSERT INTO t4_04516 VALUES (1, true), (0, false);

SELECT count() FROM t0_04516, v0_04516, t4_04516;

DROP VIEW v0_04516;
DROP TABLE t4_04516;
DROP TABLE t0_04516;
