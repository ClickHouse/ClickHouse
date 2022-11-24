-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select COUNT() from s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv') ORDER BY c1, c2, c3;
select COUNT(*) from s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv') ORDER BY c1, c2, c3;
