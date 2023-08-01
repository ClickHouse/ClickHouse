DROP TABLE IF EXISTS test_parallel_replicas_automatic_disabling;
CREATE TABLE test_parallel_replicas_automatic_disabling (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_parallel_replicas_automatic_disabling SELECT * FROM numbers(10);

SYSTEM FLUSH LOGS;

SET skip_unavailable_shards=1, allow_experimental_parallel_reading_from_replicas=1, max_parallel_replicas=3, use_hedged_requests=0, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_min_number_of_granules_to_enable=10000;
SET send_logs_level='error';
SELECT count() FROM test_parallel_replicas_automatic_disabling WHERE NOT ignore(*);

SYSTEM FLUSH LOGS;

SELECT count() > 0 FROM system.text_log WHERE event_time >= now() - INTERVAL 2 MINUTE AND message LIKE '%Parallel replicas will be disabled, because the estimated number of granules to read%';

DROP TABLE test_parallel_replicas_automatic_disabling;
