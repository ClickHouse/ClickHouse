-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select * from s3('http://localhost:11111/test/{a,b,c}.tsv') ORDER BY c1, c2, c3;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'TSV') ORDER BY c1, c2, c3;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64') ORDER BY c1, c2, c3;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest') ORDER BY c1, c2, c3;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto') ORDER BY c1, c2, c3;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV') ORDER BY c1, c2, c3;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64') ORDER BY c1, c2, c3;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto') ORDER BY c1, c2, c3;


select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv') ORDER BY c1, c2, c3;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV') ORDER BY c1, c2, c3;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64') ORDER BY c1, c2, c3;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest') ORDER BY c1, c2, c3;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto') ORDER BY c1, c2, c3;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV') ORDER BY c1, c2, c3;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64') ORDER BY c1, c2, c3;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto') ORDER BY c1, c2, c3;


desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv');
desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV');
desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64');
desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest');
desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto');
desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV');
desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64');;
desc s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto');;
