DROP TABLE IF EXISTS test_join_remote_l;
DROP TABLE IF EXISTS test_join_remote_r;

CREATE TABLE test_join_remote_l (c Int) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE test_join_remote_r (c Int) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_join_remote_l VALUES (1);
INSERT INTO test_join_remote_r VALUES (1);


SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer
SET serialize_query_plan = 0;
SET enable_parallel_replicas=1, parallel_replicas_local_plan=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT '---';
SELECT count() FROM test_join_remote_l x JOIN cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;
SELECT count() FROM test_join_remote_l x JOIN clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;
SELECT count() FROM test_join_remote_l x JOIN remote(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;
SELECT count() FROM test_join_remote_l x JOIN remoteSecure(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;

SELECT '---';
SELECT count() FROM test_join_remote_l x RIGHT JOIN cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;
SELECT count() FROM test_join_remote_l x RIGHT JOIN clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;
SELECT count() FROM test_join_remote_l x RIGHT JOIN remote(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;
SELECT count() FROM test_join_remote_l x RIGHT JOIN remoteSecure(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_r) y ON TRUE;

SELECT '---';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x JOIN test_join_remote_r y ON TRUE;
SELECT count() FROM clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x JOIN test_join_remote_r y ON TRUE;
SELECT count() FROM remote(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x JOIN test_join_remote_r y ON TRUE;
SELECT count() FROM remoteSecure(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x JOIN test_join_remote_r y ON TRUE;

SELECT '---';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x RIGHT JOIN test_join_remote_r y ON TRUE;
SELECT count() FROM clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x RIGHT JOIN test_join_remote_r y ON TRUE;
SELECT count() FROM remote(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x RIGHT JOIN test_join_remote_r y ON TRUE;
SELECT count() FROM remoteSecure(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_join_remote_l) x RIGHT JOIN test_join_remote_r y ON TRUE;

DROP TABLE test_join_remote_l;
DROP TABLE test_join_remote_r;
