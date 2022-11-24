-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

desc s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv');
desc s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv', 'TSV');
desc s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv', 'test', 'testtest');
desc s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv', 'test', 'testtest', 'TSV');

select * from s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv') order by c1, c2, c3;
select * from s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv', 'TSV') order by c1, c2, c3;
select * from s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv', 'test', 'testtest') order by c1, c2, c3;
select * from s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/{a,b}.tsv', 'test', 'testtest', 'TSV') order by c1, c2, c3; 

