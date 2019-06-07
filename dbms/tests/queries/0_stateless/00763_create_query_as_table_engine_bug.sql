drop table if exists test.t;
drop table if exists test.td;
create table test.t (val UInt32) engine = MergeTree order by val;
create table test.td engine = Distributed(test_shard_localhost, 'test', 't') as test.t;
select engine from system.tables where database = 'test' and name = 'td';
drop table if exists test.t;
drop table if exists test.td;

