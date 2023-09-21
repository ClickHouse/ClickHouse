-- Tags: no-replicated-database, no-fasttest
-- Tag no-replicated-database: different number of replicas

create table src1 (n int) engine=ReplicatedMergeTree('/test/02413/{database}/src', '1') order by tuple() settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1;
create table src2 (n int) engine=ReplicatedMergeTree('/test/02413/{database}/src', '2') order by tuple() settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1;
create table dst1 (n int) engine=ReplicatedMergeTree('/test/02413/{database}/dst', '1') order by tuple() settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1;
create table dst2 (n int) engine=ReplicatedMergeTree('/test/02413/{database}/dst', '2') order by tuple() settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1;

-- FIXME zero-copy locks may remain in ZooKeeper forever if we failed to insert a part.
-- Probably that's why we have to replace repsistent lock with ephemeral sometimes.
-- See also "Replacing persistent lock with ephemeral for path {}. It can happen only in case of local part loss"
-- in StorageReplicatedMergeTree::createZeroCopyLockNode
set insert_keeper_fault_injection_probability=0;

insert into src1 values(1);
insert into src2 values(2);
system sync replica src1 lightweight;

alter table dst1 replace partition id 'all' from src1;
system sync replica dst2;

select count() != 0 from dst1;
select count() != 0 from dst2;

-- ensure that locks exist and they are not ephemeral
set allow_unrestricted_reads_from_keeper=1;
select count(), sum(ephemeralOwner) from system.zookeeper where path like '/clickhouse/zero_copy/zero_copy_s3/' ||
    (select value from system.zookeeper where path='/test/02413/'||currentDatabase()||'/dst' and name='table_shared_id') || '/%';

-- check the same for move partition
alter table dst2 move partition id 'all' to table src2;
system sync replica src1;

select count(), sum(ephemeralOwner) from system.zookeeper where path like '/clickhouse/zero_copy/zero_copy_s3/' ||
    (select value from system.zookeeper where path='/test/02413/'||currentDatabase()||'/src' and name='table_shared_id') || '/%';
