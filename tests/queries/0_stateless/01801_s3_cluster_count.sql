-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select COUNT() from s3Cluster('test_cluster_one_shard_two_replicas', 'http://localhost:11111/test/{a,b,c}.tsv');
select COUNT(*) from s3Cluster('test_cluster_one_shard_two_replicas', 'http://localhost:11111/test/{a,b,c}.tsv');
