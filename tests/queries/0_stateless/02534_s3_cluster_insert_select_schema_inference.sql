-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

drop table if exists test;
create table test (x UInt32, y UInt32, z UInt32) engine=Memory();
insert into test select * from s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/a.tsv');
select * from test;
drop table test;

