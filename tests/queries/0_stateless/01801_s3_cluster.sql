-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select * from s3('http://localhost:11111/test/{a,b,c}.tsv') ORDER BY a, b, c;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'TSV') ORDER BY a, b, c;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'a UInt64, b UInt64, c UInt64') ORDER BY a, b, c;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest') ORDER BY a, b, c;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'a UInt64, b UInt64, c UInt64', 'auto') ORDER BY a, b, c;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV') ORDER BY a, b, c;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'a UInt64, b UInt64, c UInt64') ORDER BY a, b, c;
select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'a UInt64, b UInt64, c UInt64', 'auto') ORDER BY a, b, c;


select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'a UInt64, b UInt64, c UInt64') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'a UInt64, b UInt64, c UInt64', 'auto') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'a UInt64, b UInt64, c UInt64') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'a UInt64, b UInt64, c UInt64', 'auto') ORDER BY a, b, c;
