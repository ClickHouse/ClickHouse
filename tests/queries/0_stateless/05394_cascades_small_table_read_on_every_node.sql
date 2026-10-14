-- Every exchange starts a stage of tasks, so a plan that reads a small dimension table in parts, gathers the
-- parts and broadcasts them pays for two stages to move a few hundred rows. Reading the whole table on every
-- node costs one scan and no stage, and the cost of an exchange makes Cascades choose it.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 8;
SET max_rows_to_group_by = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
-- The plan below shows the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS t_xr_fact;
DROP TABLE IF EXISTS t_xr_dim;
CREATE TABLE t_xr_fact (d UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '';
CREATE TABLE t_xr_dim (d UInt64 STATISTICS(uniq), g UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY d SETTINGS auto_statistics_types = '';
INSERT INTO t_xr_fact SELECT number % 200000, number FROM numbers(2000000);
INSERT INTO t_xr_dim SELECT number, number % 1000 FROM numbers(200000);

EXPLAIN SELECT sum(f.v) FROM t_xr_fact AS f JOIN t_xr_dim AS d ON f.d = d.d WHERE d.g = 3;

DROP TABLE t_xr_fact;
DROP TABLE t_xr_dim;
