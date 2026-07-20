DROP TABLE IF EXISTS test;

CREATE TABLE test (k UInt64, v String)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity=1;

INSERT INTO test SELECT number, toString(number) FROM numbers(10_000);

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SET enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
-- default coordinator
SELECT count(), sum(k)
FROM test
SETTINGS log_comment = '02950_parallel_replicas_used_replicas_count';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '02950_parallel_replicas_used_replicas_count' AND type = 'QueryFinish' AND initial_query_id = query_id)  SETTINGS enable_parallel_replicas=0;

-- In order coordinator
SELECT k FROM test order by k limit 5 offset 89 SETTINGS optimize_read_in_order=1, log_comment='02950_parallel_replicas_used_replicas_count_2', merge_tree_min_rows_for_concurrent_read=1, max_threads=1;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '02950_parallel_replicas_used_replicas_count_2' AND type = 'QueryFinish' AND initial_query_id = query_id)  SETTINGS enable_parallel_replicas=0;

-- In reverse order coordinator
SELECT k FROM test order by k desc limit 5 offset 9906 SETTINGS optimize_read_in_order=1, log_comment='02950_parallel_replicas_used_replicas_count_3', merge_tree_min_rows_for_concurrent_read=1, max_threads=1;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '02950_parallel_replicas_used_replicas_count_3' AND type = 'QueryFinish' AND initial_query_id = query_id)  SETTINGS enable_parallel_replicas=0;

DROP TABLE test;
