DROP TABLE IF EXISTS tt;
CREATE TABLE tt (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt SELECT * FROM numbers(10);

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SET enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1;
SELECT count() FROM clusterAllReplicas('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), tt) settings log_comment='02875_190aed82-2423-413b-ad4c-24dcca50f65b';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ParallelReplicasUsedCount'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '02875_190aed82-2423-413b-ad4c-24dcca50f65b' AND event_date >= yesterday();

DROP TABLE tt;
