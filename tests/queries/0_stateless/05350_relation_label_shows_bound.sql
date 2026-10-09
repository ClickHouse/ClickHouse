-- A join input without a row estimate shows the rows it cannot exceed in its `EXPLAIN` label,
-- `t_lb_unk[no_stats<=100]`, instead of `~?`: `t_lb_unk` has no statistics and a filter the primary
-- index cannot use, so the estimator knows no more than the rows of the selected parts.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_move_to_prewhere = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET param__internal_join_table_stat_hints = '{"t_lb_known": {"cardinality": 1000, "distinct_keys": {"k": 1000}}}';

DROP TABLE IF EXISTS t_lb_known;
DROP TABLE IF EXISTS t_lb_unk;

-- Sparse serialization changes the bytes a read costs; the runner randomizes it.
CREATE TABLE t_lb_known (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_lb_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_lb_known SELECT number, number FROM numbers(1000);
INSERT INTO t_lb_unk SELECT number * 10, number FROM numbers(100);

EXPLAIN SELECT count() FROM t_lb_known AS a JOIN t_lb_unk AS u ON a.k = u.k WHERE u.v < 5;

DROP TABLE t_lb_known;
DROP TABLE t_lb_unk;
