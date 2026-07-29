-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- `distributed_plan_force_shuffle_aggregation` and `distributed_aggregation_memory_efficient`
-- apply to the aggregation alternatives built by the cost-based optimizer.

DROP TABLE IF EXISTS t_agg_settings;
CREATE TABLE t_agg_settings (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_agg_settings;
INSERT INTO t_agg_settings SELECT number % 1000, number FROM numbers(10000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_agg_settings": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"k": 1000}}}';

SELECT '-- 1. default: the optimizer picks partial aggregation + merge';
EXPLAIN SELECT k, sum(v) FROM t_agg_settings GROUP BY k;

SELECT '-- 2. forced shuffle: single aggregation over a shuffle exchange';
EXPLAIN SELECT k, sum(v) FROM t_agg_settings GROUP BY k SETTINGS distributed_plan_force_shuffle_aggregation = 1;

SELECT '-- 3. memory-efficient merge uses GroupingAggregatedTransform, plain merge does not';
SET log_processors_profiles = 1;
SELECT sum(s) >= 0 FROM (SELECT k, sum(v) AS s FROM t_agg_settings GROUP BY k)
  SETTINGS distributed_aggregation_memory_efficient = 1, log_comment = '04503_memory_efficient_on';
SELECT sum(s) >= 0 FROM (SELECT k, sum(v) AS s FROM t_agg_settings GROUP BY k)
  SETTINGS distributed_aggregation_memory_efficient = 0, log_comment = '04503_memory_efficient_off';

-- The log introspection below is not the subject of the test; a distributed read of the
-- constantly merging system log tables can fail on parts replaced after planning.
SET make_distributed_plan = 0;
SET enable_cascades_optimizer = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

-- The `event_time` bound keeps the log scans cheap: without it every flaky-check rerun scans all
-- the log rows accumulated by the earlier runs and the test can exceed the per-run time limit.
SELECT 'memory_efficient=1:', countIf(name = 'GroupingAggregatedTransform') > 0
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '04503_memory_efficient_on' AND type = 'QueryFinish');

SELECT 'memory_efficient=0:', countIf(name = 'GroupingAggregatedTransform') > 0
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '04503_memory_efficient_off' AND type = 'QueryFinish');

DROP TABLE t_agg_settings;
