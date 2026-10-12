-- A runtime filter added above a join input after the join order is chosen carries the estimate
-- of the input it wraps (it does not change the logical row count), and an expression merged into
-- another keeps the estimate of the one that had it. Without this `EXPLAIN estimates = 1` showed
-- the input as unknown whenever runtime filters were on.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET param__internal_join_table_stat_hints = '{"t_probe": {"cardinality": 10000, "distinct_keys": {"k": 10000}}, "t_build": {"cardinality": 100, "distinct_keys": {"k": 100}}}';

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;

CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_probe SELECT number, number FROM numbers(10000);
INSERT INTO t_build SELECT number * 100, number FROM numbers(100);

-- The probe input keeps its 10000-row estimate under the runtime filter step.
EXPLAIN estimates = 1
SELECT count()
FROM t_probe AS p
JOIN t_build AS b ON p.k = b.k
WHERE p.v < 5000;

DROP TABLE t_probe;
DROP TABLE t_build;
