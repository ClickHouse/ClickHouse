-- Tags: long, no-parallel

-- regression for MEMORY_LIMIT_EXCEEDED error because of deferred final part flush

drop table if exists data_02228;
create table data_02228 (key1 UInt32, sign Int8, s UInt64) engine = CollapsingMergeTree(sign) order by (key1) partition by key1 % 1024;
insert into data_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=0;
insert into data_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=10000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
drop table data_02228;

drop table if exists data_rep_02228;
create table data_rep_02228 (key1 UInt32, sign Int8, s UInt64) engine = ReplicatedCollapsingMergeTree('/clickhouse/{database}', 'r1', sign) order by (key1) partition by key1 % 1024;
insert into data_rep_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=0;
insert into data_rep_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=10000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
drop table data_rep_02228;
