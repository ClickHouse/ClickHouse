-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- A distributed FINAL read splits into primary-key-range layers; with more layers than tasks,
-- each task carries several lanes in its `read_bucket` parameter. The optimizer clones the read
-- step when it extracts the best plan, and the clone must keep both the coordinator-computed
-- buckets and the per-task lane grouping - losing either drops rows silently.

DROP TABLE IF EXISTS t_final_lanes;
CREATE TABLE t_final_lanes (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 64;
SYSTEM STOP MERGES t_final_lanes;

-- Eight key segments, two overlapping parts each: one FINAL layer per segment, twice as many
-- layers as the 4 tasks, so every task gets 2 lanes.
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(0, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(0, 1000);
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(1000, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(1000, 1000);
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(2000, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(2000, 1000);
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(3000, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(3000, 1000);
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(4000, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(4000, 1000);
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(5000, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(5000, 1000);
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(6000, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(6000, 1000);
INSERT INTO t_final_lanes SELECT number, number, 1 FROM numbers(7000, 1000);
INSERT INTO t_final_lanes SELECT number, number * 10, 2 FROM numbers(7000, 1000);

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
-- The hinted size makes the parallel read win; the physical table stays small.
SET param__internal_join_table_stat_hints = '{"t_final_lanes": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"k": 10000000}}}';
SET param__internal_cascades_cluster_node_count = 4;

SELECT '-- 1. the FINAL read is distributed';
SET explain_query_plan_default = 'legacy';
EXPLAIN SELECT count(), sum(v), max(ver) FROM t_final_lanes FINAL
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1;

SELECT '-- 2. results match the plain plan';
SELECT count(), sum(v), max(ver) FROM t_final_lanes FINAL
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1;
SELECT count(), sum(v), max(ver) FROM t_final_lanes FINAL
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_final_lanes;
