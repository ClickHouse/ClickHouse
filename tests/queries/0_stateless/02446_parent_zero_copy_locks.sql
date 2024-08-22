-- Tags: no-replicated-database, no-fasttest
-- Tag no-replicated-database: different number of replicas

create table rmt1 (n int, m int, k int) engine=ReplicatedMergeTree('/test/02446/{database}/rmt', '1') order by n
    settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1, old_parts_lifetime=0, cleanup_delay_period=0, max_cleanup_delay_period=1, cleanup_delay_period_random_add=1, min_bytes_for_wide_part=0;
create table rmt2 (n int, m int, k int) engine=ReplicatedMergeTree('/test/02446/{database}/rmt', '2') order by n
    settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1, old_parts_lifetime=0, cleanup_delay_period=0, max_cleanup_delay_period=1, cleanup_delay_period_random_add=1, min_bytes_for_wide_part=0;

-- FIXME zero-copy locks may remain in ZooKeeper forever if we failed to insert a part.
-- Probably that's why we have to replace persistent lock with ephemeral sometimes.
-- See also "Replacing persistent lock with ephemeral for path {}. It can happen only in case of local part loss"
-- in StorageReplicatedMergeTree::createZeroCopyLockNode
set insert_keeper_fault_injection_probability=0;

insert into rmt1 values(1, 1, 1);
insert into rmt2 values(2, 2, 2);

alter table rmt1 update m = 0 where n=0;
insert into rmt1 values(3, 3, 3);
insert into rmt2 values(4, 4, 4);
select sleepEachRow(0.5) as test_does_not_rely_on_this;

insert into rmt1 values(5, 5, 5);
alter table rmt2 update m = m * 10 where 1 settings mutations_sync=2;

-- wait for parts to be merged
select throwIf(name = 'all_0_5_1_6') from system.parts where database=currentDatabase() and table like 'rmt%' and active
format Null; -- { retry 30 until serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

system sync replica rmt2;
set optimize_throw_if_noop=1;
optimize table rmt2 final;

select 1, * from rmt1 order by n;

system sync replica rmt1;
select 2, * from rmt2 order by n;

-- wait for outdated parts to be removed (do not ignore _state column, so it will count Deleting parts as well)
select throwIf(count() = 0), groupArray(_state) from (
select *, _state from system.parts where database=currentDatabase() and table like 'rmt%' and active=0
) format Null; -- { retry 30 until serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

select *, _state from system.parts where database=currentDatabase() and table like 'rmt%' and active=0;

-- ensure that old zero copy locks were removed
set allow_unrestricted_reads_from_keeper=1;
select count(), sum(ephemeralOwner) from system.zookeeper where path like '/clickhouse/zero_copy/zero_copy_s3/' ||
    (select value from system.zookeeper where path='/test/02446/'||currentDatabase()||'/rmt' and name='table_shared_id') || '/%';

select * from system.zookeeper where path like '/clickhouse/zero_copy/zero_copy_s3/' ||
    (select value from system.zookeeper where path='/test/02446/'||currentDatabase()||'/rmt' and name='table_shared_id') || '/%'
    and path not like '%/all_0_5_2_6%';
