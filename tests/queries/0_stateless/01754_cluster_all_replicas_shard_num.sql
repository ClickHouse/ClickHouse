SELECT _shard_num FROM cluster('test_shard_localhost', system.one);
SELECT _shard_num FROM clusterAllReplicas('test_shard_localhost', system.one);

SELECT _shard_num FROM cluster('test_cluster_two_shards', system.one) ORDER BY _shard_num;
SELECT _shard_num FROM clusterAllReplicas('test_cluster_two_shards', system.one) ORDER BY _shard_num;

SELECT _shard_num FROM cluster('test_cluster_one_shard_two_replicas', system.one) ORDER BY _shard_num;
SELECT _shard_num FROM clusterAllReplicas('test_cluster_one_shard_two_replicas', system.one) ORDER BY _shard_num;
