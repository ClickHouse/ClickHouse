-- Tags: no-fasttest, no-shared-merge-tree
-- no-shared-merge-tree: depend on custom storage policy

SET send_logs_level = 'fatal';

drop table if exists rmt;
drop table if exists rmt2;

set apply_mutations_on_fly = 0;

-- Disable compact parts, because we need hardlinks in mutations.
create table rmt (n int, m int, k int) engine=ReplicatedMergeTree('/test/02432/{database}', '1') order by tuple()
    settings storage_policy = 's3_cache', allow_remote_fs_zero_copy_replication=1,
        concurrent_part_removal_threshold=1, cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0,
        max_replicated_merges_in_queue=0, max_replicated_mutations_in_queue=0, min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

insert into rmt(n, m) values (1, 42);
insert into rmt(n, m) values (2, 42);
insert into rmt(n, m) values (3, 42);
insert into rmt(n, m) values (4, 42);
insert into rmt(n, m) values (5, 42);
insert into rmt(n, m) values (6, 42);
insert into rmt(n, m) values (7, 42);
insert into rmt(n, m) values (8, 42);
insert into rmt(n, m) values (9, 42);
insert into rmt(n, m) values (0, 42);

select count(), sum(n), sum(m) from rmt;

-- Add alters in between to avoid squashing of mutations
set replication_alter_partitions_sync=0;
alter table rmt update n = n * 10 where 1;
alter table rmt modify column k UInt128;
alter table rmt update n = n + 1 where 1;
system sync replica rmt;
alter table rmt modify column k String;
alter table rmt update n = n * 10 where 1;

select count(), sum(n), sum(m) from rmt;

-- New table can assign merges/mutations and can remove old parts
create table rmt2 (n int, m int, k String) engine=ReplicatedMergeTree('/test/02432/{database}', '2') order by tuple()
    settings storage_policy = 's3_cache', allow_remote_fs_zero_copy_replication=1,
        concurrent_part_removal_threshold=1, cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0,
        min_bytes_for_wide_part=0, min_rows_for_wide_part=0, max_replicated_merges_in_queue=1,
        old_parts_lifetime=0;

alter table rmt2 modify column k Nullable(String);
alter table rmt2 update n = n + 1 where 1;

alter table rmt modify setting old_parts_lifetime=0, max_replicated_mutations_in_queue=100 settings replication_alter_partitions_sync=2;

-- Wait for mutations to finish
system sync replica rmt2;
alter table rmt2 update k = 'zero copy' where 1 settings mutations_sync=2;

-- Test does not rely on sleep, it increases probability of reproducing issues.
select sleep(3);

select count(), sum(n), sum(m) from rmt;
select count(), sum(n), sum(m) from rmt2;

-- So there will be at least 2 parts (just in case no parts are removed until drop)
insert into rmt(n) values (10);

drop table rmt;
drop table rmt2;

system flush logs;
SET max_rows_to_read = 0; -- system.text_log can be really big
select count() > 0 from system.text_log where yesterday() <= event_date and logger_name like '%' || currentDatabase() || '%' and message like '%Removing % parts from filesystem (concurrently): Parts:%';
select count() > 1, countDistinct(thread_id) > 1 from system.text_log where yesterday() <= event_date and logger_name like '%' || currentDatabase() || '%' and message like '%Removing % parts in blocks range%';
