-- Tags: no-fasttest, no-parallel, no-cpu-aarch64
-- Tag no-fasttest: Depends on Java

insert into table function hdfs('hdfs://localhost:12222/test_02536.jsonl', 'TSV') select '{"x" : {"a" : 1, "b" : 2}}' settings hdfs_truncate_on_insert=1;
drop table if exists test;
create table test (x Tuple(a UInt32, b UInt32)) engine=Memory();
insert into test select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_02536.jsonl') settings use_structure_from_insertion_table_in_table_functions=0; -- {serverError TYPE_MISMATCH}
insert into test select * from hdfsCluster('test_cluster_two_shards_localhost', 'hdfs://localhost:12222/test_02536.jsonl') settings use_structure_from_insertion_table_in_table_functions=1;
select * from test;
drop table test;

