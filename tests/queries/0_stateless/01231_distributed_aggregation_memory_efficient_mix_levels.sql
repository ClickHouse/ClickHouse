-- Tags: distributed, no-parallel

set send_logs_level = 'error';

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.shard_01231_distributed_aggregation_memory_efficient;
drop table if exists shard_1.shard_01231_distributed_aggregation_memory_efficient;
drop table if exists ma_dist;

create table shard_0.shard_01231_distributed_aggregation_memory_efficient (x UInt64) engine = MergeTree order by x;
create table shard_1.shard_01231_distributed_aggregation_memory_efficient (x UInt64) engine = MergeTree order by x;

insert into shard_0.shard_01231_distributed_aggregation_memory_efficient select * from numbers(1);
insert into shard_1.shard_01231_distributed_aggregation_memory_efficient select * from numbers(10);

create table ma_dist (x UInt64) ENGINE =  Distributed(test_cluster_two_shards_different_databases, '', 'shard_01231_distributed_aggregation_memory_efficient');

set distributed_aggregation_memory_efficient = 1;
set group_by_two_level_threshold = 2;
set max_bytes_before_external_group_by = 16;
set max_bytes_ratio_before_external_group_by = 0;

select x, count() from ma_dist group by x order by x;

select arrayFilter(y -> y = x, [x]) as f from ma_dist order by f;

drop table if exists shard_0.shard_01231_distributed_aggregation_memory_efficient;
drop table if exists shard_1.shard_01231_distributed_aggregation_memory_efficient;

drop table ma_dist;

drop database shard_0;
drop database shard_1;
