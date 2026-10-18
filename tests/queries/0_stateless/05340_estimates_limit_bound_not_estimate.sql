-- A `LIMIT` bounds the rows of its input; it does not estimate them. An input without an estimate
-- stays without one under a limit, in the join order optimizer and in the Cascades optimizer, and
-- `WITH TIES` can keep every row equal to the last one, so it bounds nothing.
SET enable_analyzer = 1;
-- The columns a step does not need are dropped below it, which narrows the rows the costs count;
-- the runner randomizes the removal.
SET query_plan_remove_unused_columns = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 3;
SET param__internal_join_table_stat_hints = '{"t_lb_known": {"cardinality": 1000, "distinct_keys": {"k": 1000}}}';

DROP TABLE IF EXISTS t_lb_known;
DROP TABLE IF EXISTS t_lb_unk;
-- Sparse serialization changes the bytes a read costs; the runner randomizes it.
CREATE TABLE t_lb_known (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_lb_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_lb_known SELECT number, number FROM numbers(1000);
INSERT INTO t_lb_unk SELECT number, number FROM numbers(100);

SELECT '-- a filtered read without an estimate under LIMIT 10 stays unknown; the limit is its bound';
EXPLAIN estimates = 1
SELECT count() FROM t_lb_known AS a JOIN (SELECT k FROM t_lb_unk WHERE v < 5 LIMIT 10) AS u ON a.k = u.k;

SELECT '-- LIMIT 10 WITH TIES over a known read bounds nothing: the subquery keeps the read estimate';
EXPLAIN estimates = 1
SELECT count() FROM t_lb_known AS a JOIN (SELECT k FROM t_lb_unk ORDER BY v LIMIT 10 WITH TIES) AS u ON a.k = u.k;

SELECT '-- the same two in the Cascades optimizer';
EXPLAIN estimates = 1
SELECT count() FROM t_lb_known AS a JOIN (SELECT k FROM t_lb_unk WHERE v < 5 LIMIT 10) AS u ON a.k = u.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1;
EXPLAIN estimates = 1
SELECT count() FROM t_lb_known AS a JOIN (SELECT k FROM t_lb_unk ORDER BY v LIMIT 10 WITH TIES) AS u ON a.k = u.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1;

DROP TABLE t_lb_known;
DROP TABLE t_lb_unk;
