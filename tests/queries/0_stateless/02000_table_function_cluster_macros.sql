SELECT _shard_num FROM cluster("{default_cluster_macro}", system.one);
SELECT _shard_num FROM clusterAllReplicas("{default_cluster_macro}", system.one);
