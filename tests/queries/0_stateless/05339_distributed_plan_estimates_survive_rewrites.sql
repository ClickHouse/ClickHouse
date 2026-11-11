-- The rule-based distributed planner moves a join, an aggregation or a read into a new node under
-- a gather exchange. The row estimate stamped on the node has to move with the step, and the
-- gather carries it too, or `EXPLAIN estimates` loses it for exactly the rewritten steps. A global
-- aggregation is estimated as well. An aggregation whose group count the statistics do not give
-- stays unknown: the input rows the strategy decision falls back to are not reported as groups.
SET enable_analyzer = 1;
-- The join cost printed below comes from the join order optimizer; the runner may turn it off.
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_default_reader_bucket_count = 2;
SET distributed_plan_max_rows_to_broadcast = 0;
SET use_statistics = 0;

DROP TABLE IF EXISTS t_sr_big;
DROP TABLE IF EXISTS t_sr_small;
CREATE TABLE t_sr_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_sr_small (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_sr_big SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_sr_small SELECT number, number FROM numbers(1000);

SELECT '-- shuffled join and distributed reads keep their estimates';
EXPLAIN estimates = 1 SELECT count() FROM t_sr_big AS b JOIN t_sr_small AS s ON b.k = s.k;

SELECT '-- shuffled aggregation with a known group count';
SET param__internal_join_table_stat_hints = '{"t_sr_big": {"cardinality": 100000, "distinct_keys": {"k": 1000}}}';
EXPLAIN estimates = 1 SELECT k, count() FROM t_sr_big GROUP BY k;

SELECT '-- shuffled aggregation without a group count: unknown, not the input rows';
SET param__internal_join_table_stat_hints = '{}';
EXPLAIN estimates = 1 SELECT k, count() FROM t_sr_big GROUP BY k;

SELECT '-- global aggregation: one row';
EXPLAIN estimates = 1 SELECT sum(v) FROM t_sr_big;

DROP TABLE t_sr_big;
DROP TABLE t_sr_small;
