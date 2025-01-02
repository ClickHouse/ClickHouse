-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: Depends on Java

insert into table function hdfs('hdfs://localhost:12222/test_1.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 1, 2, 3 settings hdfs_truncate_on_insert=1;
insert into table function hdfs('hdfs://localhost:12222/test_2.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 4, 5, 6 settings hdfs_truncate_on_insert=1;
insert into table function hdfs('hdfs://localhost:12222/test_3.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 7, 8, 9 settings hdfs_truncate_on_insert=1;

select * from hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv') order by c1, c2, c3;
select * from hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV') order by c1, c2, c3;
select * from hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32') order by c1, c2, c3;
select * from hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32', 'auto') order by c1, c2, c3;

select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv') order by c1, c2, c3;
select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV') order by c1, c2, c3;
select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32') order by c1, c2, c3;
select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32', 'auto') order by c1, c2, c3;

desc hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv');
desc hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV');
desc hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32');
desc hdfs('hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32', 'auto');

desc hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv');
desc hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV');
desc hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32');
desc hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}.tsv', 'TSV', 'c1 UInt32, c2 UInt32, c3 UInt32', 'auto');
