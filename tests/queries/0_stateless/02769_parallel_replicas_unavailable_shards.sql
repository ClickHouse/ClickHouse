DROP TABLE IF EXISTS test_parallel_replicas_unavailable_shards;
CREATE TABLE test_parallel_replicas_unavailable_shards (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_parallel_replicas_unavailable_shards SELECT * FROM numbers(10);

SYSTEM FLUSH LOGS;

SET allow_experimental_parallel_reading_from_replicas=2, max_parallel_replicas=11, use_hedged_requests=0, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1;
SET send_logs_level='error';
SELECT count() FROM test_parallel_replicas_unavailable_shards WHERE NOT ignore(*);

SYSTEM FLUSH LOGS;

SELECT count() > 0 FROM system.text_log WHERE yesterday() <= event_date AND message LIKE '%Replica number 10 is unavailable%';

DROP TABLE test_parallel_replicas_unavailable_shards;
