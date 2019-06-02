create table t (val UInt32) engine = MergeTree order by val;
create table td engine = Distributed(test_shard_localhost, 'default', 't') as t;
select engine from system.tables where database = 'default' and name = 'td';
