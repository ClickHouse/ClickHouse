-- An `INTERSECT` bounds its distinct rows by its inputs, and a `DISTINCT` above it takes a bound
-- below its own estimate as the estimate. An unknown input offers only its row bound: its row count
-- is a search value for the cost model. The second input below is a join the estimator cannot
-- estimate, with a search value of 100 rows and a bound of 20000; without this the search value
-- became the distinct bound and the `DISTINCT` a known 100 rows. The cost token is stripped from
-- the plan lines: this test is about rows.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 0;
SET param__internal_cascades_cluster_node_count = 3;
SET param__internal_join_table_stat_hints = '{"t_di_known": {"cardinality": 1000, "distinct_keys": {"k": 1000}}, "t_di_dim": {"cardinality": 1000, "distinct_keys": {"k": 1000}}}';
SET query_plan_join_swap_table = 0;
SET max_rows_to_group_by = 0;
SET intersect_default_mode = 'DISTINCT';

DROP TABLE IF EXISTS t_di_known;
DROP TABLE IF EXISTS t_di_dim;
DROP TABLE IF EXISTS t_di_unk;

CREATE TABLE t_di_known (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_di_dim (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_di_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_di_known SELECT number FROM numbers(1000);
INSERT INTO t_di_dim SELECT number FROM numbers(1000);
INSERT INTO t_di_unk SELECT number * 10, number FROM numbers(20);

SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT DISTINCT k FROM (SELECT k FROM t_di_known INTERSECT SELECT u.k FROM t_di_unk AS u JOIN t_di_dim AS d ON u.k = d.k WHERE u.v < 5)
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1
);

DROP TABLE t_di_known;
DROP TABLE t_di_dim;
DROP TABLE t_di_unk;
