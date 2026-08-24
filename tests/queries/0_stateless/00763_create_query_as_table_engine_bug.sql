drop table if exists t;
drop table if exists td;
create table t (val UInt32) engine = MergeTree order by val;
create table td engine = Distributed(test_shard_localhost, currentDatabase(), 't') as t;
select engine from system.tables where database = currentDatabase() and name = 'td';
drop table if exists t;
drop table if exists td;
