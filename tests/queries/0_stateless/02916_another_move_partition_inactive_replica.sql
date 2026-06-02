-- Tags: no-parallel

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.from_1;
drop table if exists shard_1.from_1;
drop table if exists shard_0.to;
drop table if exists shard_1.to;

create table shard_0.from_1 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_1_' || currentDatabase(), '0') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, shared_merge_tree_disable_merges_and_mutations_assignment=1;
create table shard_1.from_1 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_1_' || currentDatabase(), '1') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, shared_merge_tree_disable_merges_and_mutations_assignment=1;

system stop merges shard_0.from_1;
system stop merges shard_1.from_1;
insert into shard_0.from_1 select number + 20 from numbers(10);
insert into shard_0.from_1 select number + 30 from numbers(10);

insert into shard_0.from_1 select number + 40 from numbers(10);
insert into shard_0.from_1 select number + 50 from numbers(10);

system sync replica shard_1.from_1;

create table shard_0.to (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/to_' || currentDatabase(), '0') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, max_parts_to_merge_at_once=2, shared_merge_tree_disable_merges_and_mutations_assignment=1;

create table shard_1.to (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/to_' || currentDatabase(), '1') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, max_parts_to_merge_at_once=2;

detach table shard_1.to;

alter table shard_0.from_1 on cluster test_cluster_two_shards_different_databases move partition tuple() to table shard_0.to format Null settings distributed_ddl_output_mode='never_throw', distributed_ddl_task_timeout = 1;

drop table if exists shard_0.from_1;
drop table if exists shard_1.from_1;
OPTIMIZE TABLE shard_0.to;
OPTIMIZE TABLE shard_0.to;
select name, active from system.parts where database='shard_0' and table='to' and active order by name;

-- If moved parts are not merged by OPTIMIZE or background merge restart
-- can log Warning about metadata version on disk. It's normal situation
-- and test shouldn't rarely fail because of it.
set send_logs_level = 'error';

system restart replica shard_0.to;

-- Doesn't lead to test flakyness, because we don't check content in table
-- which doesn't depend on any background operation
select sleep(3);

attach table shard_1.to;
system sync replica shard_1.to;
select count(), sum(x) from shard_1.to;

drop table if exists shard_0.to;
drop table if exists shard_1.to;
