DROP TABLE IF EXISTS test_parallel_replicas_unavailable_shards;
CREATE TABLE test_parallel_replicas_unavailable_shards (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_parallel_replicas_unavailable_shards SELECT * FROM numbers(10);

SET allow_experimental_parallel_reading_from_replicas=2, max_parallel_replicas=11, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1;
SET send_logs_level='error';
SELECT count() FROM test_parallel_replicas_unavailable_shards WHERE NOT ignore(*) SETTINGS log_comment = '02769_7b513191-5082-4073-8568-53b86a49da79';

SYSTEM FLUSH LOGS;

SET allow_experimental_parallel_reading_from_replicas=0;
SELECT ProfileEvents['ParallelReplicasUnavailableCount'] FROM system.query_log WHERE yesterday() <= event_date AND query_id in (select query_id from system.query_log where log_comment = '02769_7b513191-5082-4073-8568-53b86a49da79' and current_database = currentDatabase()) and type = 'QueryFinish' and query_id == initial_query_id;

DROP TABLE test_parallel_replicas_unavailable_shards;
