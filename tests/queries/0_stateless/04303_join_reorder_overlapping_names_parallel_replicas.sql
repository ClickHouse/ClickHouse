-- Tags: no-parallel-replicas
-- ^ The test enables parallel replicas explicitly, it should not be wrapped into another layer.

-- Regression test for a LOGICAL_ERROR ("Left and right columns have same names")
-- in the join order optimizer. With parallel replicas the query is re-analyzed for the
-- shard and table identifiers can be reused, so two relations in the join graph end up with
-- overlapping column names. Reconstructing the join with `JoinExpressionActions` requires
-- unique names across the two sides, so the optimizer must skip reordering in that case
-- instead of failing.
-- https://github.com/ClickHouse/ClickHouse/issues/89166
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=3fca01d3c822b27f51779bda78ba8afac8ffc460&name_0=MasterCI&name_1=Stress%20test%20%28arm_release%29

DROP TABLE IF EXISTS t_jr_04303_left;
DROP TABLE IF EXISTS t_jr_04303_right;

CREATE TABLE t_jr_04303_left (id UInt64, val String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_jr_04303_right (id UInt64, val String) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_jr_04303_right VALUES (2, 'R2'), (3, 'R3'), (5, 'R5'), (6, 'R6'), (7, 'R7');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET query_plan_optimize_join_order_algorithm = 'greedy';

-- The left table is empty, so the result is empty; the point is that the query must not
-- fail with a LOGICAL_ERROR during query plan optimization.
SELECT r.id, r.val FROM t_jr_04303_left AS l INNER JOIN t_jr_04303_right AS r ON l.id = r.id ORDER BY r.id;

-- Non-empty left side, same shape.
INSERT INTO t_jr_04303_left VALUES (2, 'L2'), (5, 'L5'), (9, 'L9');
SELECT r.id, r.val FROM t_jr_04303_left AS l INNER JOIN t_jr_04303_right AS r ON l.id = r.id ORDER BY r.id;

DROP TABLE t_jr_04303_left;
DROP TABLE t_jr_04303_right;
