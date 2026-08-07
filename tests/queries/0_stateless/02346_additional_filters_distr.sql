-- Tags: no-parallel, distributed

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists dist_02346;
drop table if exists shard_0.data_02346;
drop table if exists shard_1.data_02346;

create table shard_0.data_02346 (x UInt32, y String) engine = MergeTree order by x settings index_granularity = 2;
insert into shard_0.data_02346 values (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');

create table shard_1.data_02346 (x UInt32, y String) engine = MergeTree order by x settings index_granularity = 2;
insert into shard_1.data_02346 values (5, 'a'), (6, 'bb'), (7, 'ccc'), (8, 'dddd');

create table dist_02346 (x UInt32, y String) engine=Distributed('test_cluster_two_shards_different_databases', /* default_database= */ '', data_02346);

set max_rows_to_read=4;

select * from dist_02346 order by x settings additional_table_filters={'dist_02346' : 'x > 3 and x < 7'};
