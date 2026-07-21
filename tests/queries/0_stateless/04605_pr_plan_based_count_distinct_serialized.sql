-- Tags: no-parallel-replicas
-- Regression test for a crash in plan-based parallel replicas: count(DISTINCT ...) with
-- count_distinct_optimization becomes a keys-only partial aggregation (GROUP BY with no aggregate
-- function). When that partial aggregation is shipped to replicas as a serialized plan fragment, the
-- received chunks must carry AggregatedChunkInfo so the MergingAggregated above ReadFromParallelReplicas
-- can merge them. RemoteSource auto-detects this only from an AggregateFunction column in the header,
-- which a keys-only aggregation does not have -- so ReadFromParallelReplicas must set add_agg_info
-- explicitly when the fragment ends in a partial aggregation. Otherwise the server aborted with
-- "Chunk should have AggregatedChunkInfo in MergingAggregatedTransform". See PR #111063.
-- The settings that trigger the keys-only fragment (serialize_query_plan, count_distinct_optimization,
-- forced two-level aggregation) are pinned so the regression is caught deterministically.

DROP TABLE IF EXISTS t_pr_cd;

CREATE TABLE t_pr_cd (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_cd SELECT number, number % 10 FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;
SET serialize_query_plan = 1;
SET count_distinct_optimization = 1;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

SELECT count(DISTINCT b) FROM t_pr_cd;
SELECT count(DISTINCT a) FROM t_pr_cd;

DROP TABLE t_pr_cd;
