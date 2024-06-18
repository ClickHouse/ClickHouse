-- Tags: no-fasttest

create table rmt1 (n int, m int, k int) engine=ReplicatedMergeTree('/test/02485/{database}/rmt', '1') order by n
    settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1, old_parts_lifetime=60, cleanup_delay_period=60, max_cleanup_delay_period=60, cleanup_delay_period_random_add=1, min_bytes_for_wide_part=0, simultaneous_parts_removal_limit=1;
create table rmt2 (n int, m int, k int) engine=ReplicatedMergeTree('/test/02485/{database}/rmt', '2') order by n
    settings storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1, old_parts_lifetime=0, cleanup_delay_period=0, max_cleanup_delay_period=1, cleanup_delay_period_random_add=1, min_bytes_for_wide_part=0;

insert into rmt1 values (1, 1, 1);
insert into rmt1 values (2, 2, 2);
system sync replica rmt2 lightweight;

system stop merges rmt2;
system stop cleanup rmt1;
system stop replicated sends rmt1;

alter table rmt1 modify setting fault_probability_before_part_commit=1;
alter table rmt1 update k = 0 where 0;

-- give rmt1 a chance to execute MUTATE_PART (and fail)
select sleep(1) as test_does_not_rely_on_this format Null;
system stop merges rmt1;
system start merges rmt2;

system sync replica rmt2;

-- give rmt2 a chance to cleanup the source part (mutation parent)
select sleep(3) as test_does_not_rely_on_this format Null;

-- it will remove the mutated part that it failed to commit
drop table rmt1 sync;

select * from rmt2 order by n;
