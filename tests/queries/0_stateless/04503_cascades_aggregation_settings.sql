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
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_agg_settings": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"k": 1000}}}';

SELECT '-- 1. default: the optimizer picks partial aggregation + merge';
EXPLAIN SELECT k, sum(v) FROM t_agg_settings GROUP BY k;

SELECT '-- 2. forced shuffle: single aggregation over a shuffle exchange';
EXPLAIN SELECT k, sum(v) FROM t_agg_settings GROUP BY k SETTINGS distributed_plan_force_shuffle_aggregation = 1;

SELECT '-- 3. memory-efficient merge is planned as a bucket-ordered merge, plain merge is not';
-- The outer query reads the plan text through `viewExplain`, which distributed Cascades planning
-- rejects, so the outer level turns it off; the EXPLAIN'd query re-enables it explicitly.
SELECT 'memory_efficient=1:', countIf(explain LIKE '%Mode: memory-efficient%') > 0 FROM (
    EXPLAIN actions = 1 SELECT k, sum(v) FROM t_agg_settings GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
        distributed_aggregation_memory_efficient = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT 'memory_efficient=0:', countIf(explain LIKE '%Mode: memory-efficient%') > 0 FROM (
    EXPLAIN actions = 1 SELECT k, sum(v) FROM t_agg_settings GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
        distributed_aggregation_memory_efficient = 0
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_agg_settings;
