-- Tags: shard, no-parallel

create database if not exists shard_0;
create database if not exists shard_1;
drop table if exists shard_0.tbl;
drop table if exists shard_1.tbl;
drop table if exists distr;

create table shard_0.tbl (number UInt64) engine = MergeTree order by number;
create table shard_1.tbl (number UInt64) engine = MergeTree order by number;
create table distr (number UInt64) engine = Distributed(test_cluster_two_shards_different_databases, '', tbl);

set insert_distributed_sync = 1;
set insert_distributed_one_random_shard = 1;
set max_block_size = 1;
set max_insert_block_size = 1;
set min_insert_block_size_rows = 1;
insert into distr select number from numbers(100);

select count() != 0 from shard_0.tbl;
select count() != 0 from shard_1.tbl;
select * from distr order by number LIMIT 20;

drop table if exists shard_0.tbl;
drop table if exists shard_1.tbl;
drop database shard_0;
drop database shard_1;
drop table distr;
