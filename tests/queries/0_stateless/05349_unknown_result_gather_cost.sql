-- An exchange that funnels an unknown result through one node is priced by the rows that result
-- cannot exceed, capped at the largest known table of the query, never by the search value. The
-- join below is unknown because `t_ug_unk` is: its search value is the product of its inputs times
-- the key selectivity, a hundred rows, while its bound is ten million. Without this the gather of
-- that result cost next to nothing and the plan gathered it to join the ten-row `t_ug_small`
-- locally instead of broadcasting the ten rows. The cost token is stripped from the plan lines.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET param__internal_cascades_cluster_node_count = 3;
-- A small exchange cost, so that these small tables are distributed at all: the test checks
-- how the gather of an unknown result is priced.
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead":3000}';
SET param__internal_join_table_stat_hints = '{"t_ug_big": {"cardinality": 100000, "distinct_keys": {"k": 100000, "m": 100}}, "t_ug_small": {"cardinality": 10, "distinct_keys": {"m": 10}}}';
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_ug_big;
DROP TABLE IF EXISTS t_ug_unk;
DROP TABLE IF EXISTS t_ug_small;

CREATE TABLE t_ug_big (k UInt64, m UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_ug_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_ug_small (m UInt64) ENGINE = MergeTree ORDER BY m SETTINGS auto_statistics_types = '';
INSERT INTO t_ug_big SELECT number, number % 100 FROM numbers(1000);
INSERT INTO t_ug_unk SELECT number * 10, number FROM numbers(100);
INSERT INTO t_ug_small SELECT number FROM numbers(10);

SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT count()
FROM t_ug_big AS b
JOIN t_ug_unk AS u ON b.k = u.k
JOIN t_ug_small AS s ON b.m = s.m
WHERE u.v < 5
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1
);

DROP TABLE t_ug_big;
DROP TABLE t_ug_unk;
DROP TABLE t_ug_small;
