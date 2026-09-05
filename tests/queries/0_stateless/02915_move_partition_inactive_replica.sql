-- Tags: no-flaky-check

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.from_0_02915;
drop table if exists shard_1.from_0_02915;
drop table if exists shard_0.from_1_02915;
drop table if exists shard_1.from_1_02915;
drop table if exists shard_0.to_02915;
drop table if exists shard_1.to_02915;

create table shard_0.from_0_02915 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_0_02915_' || currentDatabase(), '0') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1;
create table shard_1.from_0_02915 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_0_02915_' || currentDatabase(), '1') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1;

create table shard_0.from_1_02915 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_1_02915_' || currentDatabase(), '0') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1;
create table shard_1.from_1_02915 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_1_02915_' || currentDatabase(), '1') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1;

insert into shard_0.from_0_02915 select number from numbers(10);
insert into shard_0.from_0_02915 select number + 10 from numbers(10);

insert into shard_0.from_1_02915 select number + 20 from numbers(10);
insert into shard_0.from_1_02915 select number + 30 from numbers(10);

system sync replica shard_1.from_0_02915;
system sync replica shard_1.from_1_02915;


create table shard_0.to_02915 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/to_02915_' || currentDatabase(), '0') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1;

create table shard_1.to_02915 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/to_02915_' || currentDatabase(), '1') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1;

detach table shard_1.to_02915;

alter table shard_0.from_0_02915 on cluster test_cluster_two_shards_different_databases move partition tuple() to table shard_0.to_02915 format Null settings distributed_ddl_output_mode='never_throw', distributed_ddl_task_timeout = 1;

alter table shard_0.from_1_02915 on cluster test_cluster_two_shards_different_databases move partition tuple() to table shard_0.to_02915 format Null settings distributed_ddl_output_mode='never_throw', distributed_ddl_task_timeout = 1;

OPTIMIZE TABLE shard_0.from_0_02915;
OPTIMIZE TABLE shard_1.from_0_02915;
OPTIMIZE TABLE shard_0.from_1_02915;
OPTIMIZE TABLE shard_1.from_1_02915;

OPTIMIZE TABLE shard_0.to_02915;

-- If moved parts are not merged by OPTIMIZE or background merge restart
-- can log Warning about metadata version on disk. It's normal situation
-- and test shouldn't rarely fail because of it.
set send_logs_level = 'error';

system restart replica shard_0.to_02915;

-- Doesn't lead to test flakyness, because we don't check anything after it
select sleep(2);

attach table shard_1.to_02915;

drop table if exists shard_0.from_0_02915;
drop table if exists shard_1.from_0_02915;
drop table if exists shard_0.from_1_02915;
drop table if exists shard_1.from_1_02915;
drop table if exists shard_0.to_02915;
drop table if exists shard_1.to_02915;
