-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

desc s3Cluster(test_cluster_one_shard_three_replicas_localhost, 'http://localhost:11111/test/02876.parquet');
select * from s3Cluster(test_cluster_one_shard_three_replicas_localhost, 'http://localhost:11111/test/02876.parquet');

