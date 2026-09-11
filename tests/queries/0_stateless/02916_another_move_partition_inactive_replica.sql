-- Tags: no-flaky-check

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.from_1_02916;
drop table if exists shard_1.from_1_02916;
drop table if exists shard_0.to_02916;
drop table if exists shard_1.to_02916;

create table shard_0.from_1_02916 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_1_02916_' || currentDatabase(), '0') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, shared_merge_tree_disable_merges_and_mutations_assignment=1;
create table shard_1.from_1_02916 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/from_1_02916_' || currentDatabase(), '1') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, shared_merge_tree_disable_merges_and_mutations_assignment=1;

system stop merges shard_0.from_1_02916;
system stop merges shard_1.from_1_02916;
insert into shard_0.from_1_02916 select number + 20 from numbers(10);
insert into shard_0.from_1_02916 select number + 30 from numbers(10);

insert into shard_0.from_1_02916 select number + 40 from numbers(10);
insert into shard_0.from_1_02916 select number + 50 from numbers(10);

system sync replica shard_1.from_1_02916;

create table shard_0.to_02916 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/to_02916_' || currentDatabase(), '0') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, max_parts_to_merge_at_once=2, merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once=0, shared_merge_tree_disable_merges_and_mutations_assignment=1;

create table shard_1.to_02916 (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/to_02916_' || currentDatabase(), '1') order by x settings old_parts_lifetime=1, max_cleanup_delay_period=1, cleanup_delay_period=1, max_parts_to_merge_at_once=2, merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once=0;

detach table shard_1.to_02916;

alter table shard_0.from_1_02916 on cluster test_cluster_two_shards_different_databases move partition tuple() to table shard_0.to_02916 format Null settings distributed_ddl_output_mode='never_throw', distributed_ddl_task_timeout = 1;

drop table if exists shard_0.from_1_02916;
drop table if exists shard_1.from_1_02916;
OPTIMIZE TABLE shard_0.to_02916;
OPTIMIZE TABLE shard_0.to_02916;
select count() from system.parts where database='shard_0' and table='to_02916' and active;

-- If moved parts are not merged by OPTIMIZE or background merge restart
-- can log Warning about metadata version on disk. It's normal situation
-- and test shouldn't rarely fail because of it.
set send_logs_level = 'error';

system restart replica shard_0.to_02916;

-- Doesn't lead to test flakyness, because we don't check content in table
-- which doesn't depend on any background operation
select sleep(3);

attach table shard_1.to_02916;
system sync replica shard_1.to_02916;
select count(), sum(x) from shard_1.to_02916;

drop table if exists shard_0.to_02916;
drop table if exists shard_1.to_02916;
