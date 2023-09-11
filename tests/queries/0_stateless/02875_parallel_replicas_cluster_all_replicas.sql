DROP TABLE IF EXISTS tt;
CREATE TABLE tt (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt SELECT * FROM numbers(10);

SET allow_experimental_parallel_reading_from_replicas=1, max_parallel_replicas=3, use_hedged_requests=0, parallel_replicas_for_non_replicated_merge_tree=1;
SELECT count() FROM clusterAllReplicas('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), tt);

SYSTEM FLUSH LOGS;

SET allow_experimental_parallel_reading_from_replicas=0;
SELECT count() > 0 FROM system.text_log WHERE event_time >= now() - INTERVAL 2 MINUTE AND message LIKE '%Parallel reading from replicas is disabled for shard. Not enough nodes%';

DROP TABLE tt;
