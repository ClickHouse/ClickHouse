SELECT _shard_num FROM cluster("{default_cluster_macro}", system.one);
SELECT _shard_num FROM cluster("{default_cluster_macro}");
SELECT _shard_num FROM clusterAllReplicas("{default_cluster_macro}", system.one);
SELECT _shard_num FROM clusterAllReplicas("{default_cluster_macro}");

SELECT _shard_num FROM cluster("{nonexistent}"); -- { serverError BAD_ARGUMENTS }
SELECT _shard_num FROM cluster("{nonexistent}", system.one); -- { serverError BAD_ARGUMENTS }
SELECT _shard_num FROM clusterAllReplicas("{nonexistent}"); -- { serverError BAD_ARGUMENTS }
SELECT _shard_num FROM clusterAllReplicas("{nonexistent}", system.one); -- { serverError BAD_ARGUMENTS }
