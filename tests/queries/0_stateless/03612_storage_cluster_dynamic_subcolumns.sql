-- Tags: no-fasttest

drop table if exists test;
drop table if exists test_cluster;
create table test (json JSON, d Dynamic) engine=MergeTree order by tuple();
insert into test select '{"a" : 42}', 42::Int64;
create table test_cluster as cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 'test');
select * from test_cluster;
select json.a, d.Int64 from test_cluster;
drop table test_cluster;
drop table test;

