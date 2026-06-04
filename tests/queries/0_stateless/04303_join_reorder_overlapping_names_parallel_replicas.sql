-- Tags: no-parallel-replicas
-- ^ The test sets the parallel replicas settings explicitly below, so it should not be wrapped
--   into another layer of parallel replicas by the test runner.

-- Regression test for a LOGICAL_ERROR ("Left and right columns have same names")
-- in the join order optimizer. When the join is executed with parallel replicas, the
-- query is re-analyzed for the shard and table identifiers can be reused, so two relations
-- in the join graph may end up with overlapping column names. Reconstructing the join with
-- `JoinExpressionActions` requires unique names across the two sides, so the optimizer must
-- skip reordering in that case instead of failing.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=3fca01d3c822b27f51779bda78ba8afac8ffc460&name_0=MasterCI&name_1=Stress%20test%20%28arm_release%29

DROP TABLE IF EXISTS t_jr_04303_1;
DROP TABLE IF EXISTS t_jr_04303_2;
DROP TABLE IF EXISTS t_jr_04303_3;

-- All tables share the same column names (a, b) - this is the shape that triggered the crash.
CREATE TABLE t_jr_04303_1 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_jr_04303_2 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_jr_04303_3 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_jr_04303_1 SELECT number, number FROM numbers(1000);
INSERT INTO t_jr_04303_2 SELECT number, number FROM numbers(1000);
INSERT INTO t_jr_04303_3 SELECT number, number FROM numbers(2000);

SET enable_analyzer = 1;
SET join_algorithm = 'parallel_hash';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

-- Run with a few different join-order optimizer randomization seeds: the results must be
-- correct and the query must not fail regardless of the chosen join order.
SET query_plan_optimize_join_order_randomize = 1;
SELECT count() FROM t_jr_04303_2 AS x INNER JOIN t_jr_04303_1 AS y ON x.a = y.a;
SELECT count() FROM t_jr_04303_1 AS x INNER JOIN t_jr_04303_2 AS y ON x.a = y.a INNER JOIN t_jr_04303_3 AS z ON y.b = z.b;

SET query_plan_optimize_join_order_randomize = 42;
SELECT count() FROM t_jr_04303_2 AS x INNER JOIN t_jr_04303_1 AS y ON x.a = y.a;
SELECT count() FROM t_jr_04303_1 AS x INNER JOIN t_jr_04303_2 AS y ON x.a = y.a INNER JOIN t_jr_04303_3 AS z ON y.b = z.b;

SET query_plan_optimize_join_order_randomize = 314159;
SELECT count() FROM t_jr_04303_2 AS x INNER JOIN t_jr_04303_1 AS y ON x.a = y.a;
SELECT count() FROM t_jr_04303_1 AS x INNER JOIN t_jr_04303_2 AS y ON x.a = y.a INNER JOIN t_jr_04303_3 AS z ON y.b = z.b;

DROP TABLE t_jr_04303_1;
DROP TABLE t_jr_04303_2;
DROP TABLE t_jr_04303_3;
