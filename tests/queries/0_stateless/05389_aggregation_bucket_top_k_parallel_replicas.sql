-- Parallel replicas with a local plan aggregate partially on every replica, the initiator among them, and finish the
-- aggregation on the initiator. The top-K pruning of the adaptive aggregator needs a final aggregation, so a partial
-- one keeps every group with its whole count. Keys 0 to 9 take 100000 rows each and the other keys 200 each, so a
-- group that a replica drops or undercounts changes the result.

DROP TABLE IF EXISTS t_bucket_top_k_parallel_replicas;
CREATE TABLE t_bucket_top_k_parallel_replicas (k UInt64, n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_bucket_top_k_parallel_replicas SELECT if(number % 100 < 50, number % 10, number % 10000), number FROM numbers(2000000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0, serialize_query_plan = 0, distributed_aggregation_memory_efficient = 0;
SET query_plan_aggregation_bucket_top_k = 1, max_threads = 4, max_threads_min_free_memory_per_thread = 0;
-- The adaptive tables freeze at once, and neither a group limit, hash-table statistics nor spilling disarm the top-K
-- pruning.
SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0, max_rows_to_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT arraySort(groupArray((k, c))) FROM (SELECT k, count() AS c FROM t_bucket_top_k_parallel_replicas GROUP BY k ORDER BY c DESC LIMIT 10);

DROP TABLE t_bucket_top_k_parallel_replicas;
