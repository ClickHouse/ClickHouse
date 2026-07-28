-- Tags: no-parallel-replicas
-- Regression test for an exception in plan-based parallel replicas. count(DISTINCT ...) with
-- count_distinct_optimization is a keys-only aggregation shipped to replicas as a plan fragment; its
-- chunks must carry AggregatedChunkInfo or the MergingAggregated above ReadFromParallelReplicas throws
-- ("Chunk should have AggregatedChunkInfo in MergingAggregatedTransform"). See PR #111063.
-- count_distinct_optimization and forced two-level aggregation are pinned to trigger it deterministically.

DROP TABLE IF EXISTS t_pr_cd;

CREATE TABLE t_pr_cd (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_cd SELECT number, number % 10 FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET count_distinct_optimization = 1;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

SELECT count(DISTINCT b) FROM t_pr_cd;
SELECT count(DISTINCT a) FROM t_pr_cd;

DROP TABLE t_pr_cd;
