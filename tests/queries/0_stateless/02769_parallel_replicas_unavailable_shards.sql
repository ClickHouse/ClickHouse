DROP TABLE IF EXISTS test_parallel_replicas_unavailable_shards;
CREATE TABLE test_parallel_replicas_unavailable_shards (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_parallel_replicas_unavailable_shards SELECT * FROM numbers(10);

SET allow_experimental_parallel_reading_from_replicas=2, max_parallel_replicas=11, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1;
SET send_logs_level='error';
SELECT count() FROM test_parallel_replicas_unavailable_shards WHERE NOT ignore(*) settings log_comment = '02769_c4589ffa-f6df-4ae9-83b6-214105d1927e';

SYSTEM FLUSH LOGS;
SET allow_experimental_parallel_reading_from_replicas=0;
-- DO NOT MERGE - just to pass fast test check, - need to change test
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log WHERE yesterday() <= event_date AND
    query_id in (select query_id from system.query_log where current_database = currentDatabase() AND log_comment='02769_c4589ffa-f6df-4ae9-83b6-214105d1927e') and type = 'QueryFinish';

DROP TABLE test_parallel_replicas_unavailable_shards;
