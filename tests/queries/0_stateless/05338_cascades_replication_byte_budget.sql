-- Every replication the Cascades optimizer proposes, a broadcast join side, a replicated read or a
-- replicated subplan, has to fit `distributed_plan_max_bytes_to_broadcast`: the estimated rows
-- times the row width when the rows are known and no default went into them, the proven row bound
-- otherwise; a result with neither is never replicated. Without the budget the cost model alone
-- would decide. The hints stand in for statistics; side widths stay small and the budget moves, so
-- the cost model has no reason to copy the other side.
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 0;
SET param__internal_cascades_cluster_node_count = 3;
-- A small exchange cost, so that these small tables are distributed at all: the test checks
-- which replications fit the byte budget.
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead":3000}';

DROP TABLE IF EXISTS t_cb_fact;
DROP TABLE IF EXISTS t_cb_dim;
DROP TABLE IF EXISTS t_cb_unk;
DROP TABLE IF EXISTS t_cb_scan;
DROP TABLE IF EXISTS t_cb_wide;
CREATE TABLE t_cb_scan (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_cb_scan SELECT number % 5, number FROM numbers(100000);
CREATE TABLE t_cb_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_cb_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_cb_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_cb_fact SELECT number % 5, number FROM numbers(1000);
INSERT INTO t_cb_dim SELECT number, concat('nm_', toString(number)) FROM numbers(5);
INSERT INTO t_cb_unk SELECT number, number FROM numbers(100);
CREATE TABLE t_cb_wide (k UInt64, name String) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_cb_wide SELECT number, repeat('w', 2000) FROM numbers(5);

SELECT '-- 5 rows of 18 bytes fit the budget: the dimension is broadcast';
SET param__internal_join_table_stat_hints = '{"t_cb_fact": {"cardinality": 10000000, "avg_row_bytes": 16, "distinct_keys": {"k": 5}}, "t_cb_dim": {"cardinality": 5, "avg_row_bytes": 18, "distinct_keys": {"k": 5}}}';
EXPLAIN SELECT count(), any(name) FROM t_cb_fact AS f JOIN t_cb_dim AS d ON f.k = d.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1;

SELECT '-- a budget of 50 bytes fits neither side: no replication, the join shuffles';
EXPLAIN SELECT count(), any(name) FROM t_cb_fact AS f JOIN t_cb_dim AS d ON f.k = d.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_max_bytes_to_broadcast = 50;

SELECT '-- a filter the primary key cannot prune leaves the rows unknown; the bound of 100 rows times the 8 bytes of the key fits a budget of 2000 bytes';
SET param__internal_join_table_stat_hints = '{"t_cb_fact": {"cardinality": 10000000, "avg_row_bytes": 16, "distinct_keys": {"k": 5}}}';
EXPLAIN SELECT count() FROM t_cb_fact AS f JOIN t_cb_unk AS u ON f.k = u.k WHERE u.v < 5
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_max_bytes_to_broadcast = 2000;

SELECT '-- and does not fit a budget of 500 bytes: no replication of the unknown side';
EXPLAIN SELECT count() FROM t_cb_fact AS f JOIN t_cb_unk AS u ON f.k = u.k WHERE u.v < 5
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_max_bytes_to_broadcast = 500;

SELECT '-- a small result over a large scan: the broadcast ships the result and fits, the scan is read once';
-- 100000 rows are scanned for 5 hinted rows; a budget of 2000 bytes fits the 5 rows, not the scan.
SET param__internal_join_table_stat_hints = '{"t_cb_fact": {"cardinality": 10000000, "avg_row_bytes": 16, "distinct_keys": {"k": 5}}, "t_cb_scan": {"cardinality": 5, "avg_row_bytes": 16, "distinct_keys": {"k": 5}}}';
EXPLAIN SELECT count() FROM t_cb_fact AS f JOIN (SELECT k FROM t_cb_scan WHERE v = 7) AS s ON f.k = s.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_max_bytes_to_broadcast = 2000;

SELECT '-- a default in one of two AND-ed predicates of a filter makes the side a guess: its bound does not fit, no broadcast';
-- The filter stays above the aggregation (no push-down), so the Cascades optimizer estimates it
-- itself: the `LIKE` with its default, `k = 3` from the key NDV. The 0.1 estimated rows would fit any
-- budget; the bound, the aggregated rows, does not fit 2000 bytes.
SET param__internal_join_table_stat_hints = '{"t_cb_fact": {"cardinality": 10000000, "avg_row_bytes": 16, "distinct_keys": {"k": 5}}, "t_cb_scan": {"cardinality": 100000, "avg_row_bytes": 16, "distinct_keys": {"k": 5, "v": 100000}}}';
EXPLAIN SELECT count() FROM t_cb_fact AS f
JOIN (SELECT k FROM (SELECT k, count() AS c FROM t_cb_scan GROUP BY k) WHERE toString(k) LIKE '%3%' AND k = 3) AS s ON f.k = s.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_max_bytes_to_broadcast = 2000, query_plan_filter_push_down = 0;

SELECT '-- a UNION ALL is as wide as its inputs weighted by their rows: 5 rows of 18 and 5 of 2000 bytes do not fit a budget of 2000 bytes';
-- Without the weighting the union took the first input's 18 bytes and its 10 rows fit.
SET param__internal_join_table_stat_hints = '{"t_cb_fact": {"cardinality": 10000000, "avg_row_bytes": 16, "distinct_keys": {"k": 5}}, "t_cb_dim": {"cardinality": 5, "avg_row_bytes": 18, "distinct_keys": {"k": 5}}, "t_cb_wide": {"cardinality": 5, "avg_row_bytes": 2000, "distinct_keys": {"k": 5}}}';
EXPLAIN SELECT count() FROM t_cb_fact AS f JOIN (SELECT k FROM t_cb_dim UNION ALL SELECT k FROM t_cb_wide) AS u ON f.k = u.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_max_bytes_to_broadcast = 2000;

SELECT '-- results do not depend on the shape';
SELECT count(), min(name) FROM t_cb_fact AS f JOIN t_cb_dim AS d ON f.k = d.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1;
SELECT count() FROM t_cb_fact AS f JOIN t_cb_unk AS u ON f.k = u.k WHERE u.v < 5
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1, distributed_plan_max_bytes_to_broadcast = 500;

DROP TABLE t_cb_fact;
DROP TABLE t_cb_dim;
DROP TABLE t_cb_unk;
DROP TABLE t_cb_scan;
DROP TABLE t_cb_wide;
