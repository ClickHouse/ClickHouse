-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Java

insert into table function hdfs('hdfs://localhost:12222/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 1, 2, 3 settings hdfs_truncate_on_insert=1;
insert into table function hdfs('hdfs://localhost:12222/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 4, 5, 6 settings hdfs_truncate_on_insert=1;
insert into table function hdfs('hdfs://localhost:12222/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32') select 7, 8, 9 settings hdfs_truncate_on_insert=1;

select * from hdfs('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}');
select * from hdfs('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32');

select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}');
select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_{1,2,3}', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
