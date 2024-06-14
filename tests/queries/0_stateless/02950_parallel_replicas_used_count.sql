DROP TABLE IF EXISTS test;

CREATE TABLE test (k UInt64, v String)
ENGINE = MergeTree
ORDER BY k;

INSERT INTO test SELECT number, toString(number) FROM numbers(100);

SET use_parallel_replicas = 2, max_parallel_replicas = 3, prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

-- default coordinator
SELECT count(), sum(k)
FROM test
SETTINGS log_comment = '02950_parallel_replicas_used_replicas_count';

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['ParallelReplicasUsedCount'] FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '02950_parallel_replicas_used_replicas_count' AND type = 'QueryFinish' AND initial_query_id = query_id)  SETTINGS use_parallel_replicas=0;

-- In order coordinator
SELECT k FROM test order by k limit 5 offset 89 SETTINGS optimize_read_in_order=1, log_comment='02950_parallel_replicas_used_replicas_count_2';

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['ParallelReplicasUsedCount'] FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '02950_parallel_replicas_used_replicas_count_2' AND type = 'QueryFinish' AND initial_query_id = query_id)  SETTINGS use_parallel_replicas=0;

DROP TABLE test;
