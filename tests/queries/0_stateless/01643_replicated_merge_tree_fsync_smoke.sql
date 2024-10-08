-- Tags: no-parallel, no-object-storage
-- no-parallel -- for flaky check and to avoid "Removing leftovers from table" (for other tables)

-- Temporarily skip warning 'table was created by another server at the same moment, will retry'
set send_logs_level='error';
set database_atomic_wait_for_drop_and_detach_synchronously=1;

drop table if exists rep_fsync_r1;
drop table if exists rep_fsync_r2;

select 'default';
create table rep_fsync_r1 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r1') order by key;
create table rep_fsync_r2 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r2') order by key;
insert into rep_fsync_r1 values (1);
system sync replica rep_fsync_r2;
select * from rep_fsync_r2;
optimize table rep_fsync_r1 final;
system sync replica rep_fsync_r2;
drop table rep_fsync_r1;
drop table rep_fsync_r2;

select 'compact fsync_after_insert';
create table rep_fsync_r1 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r1') order by key settings min_rows_for_wide_part=2, fsync_after_insert=1;
create table rep_fsync_r2 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r2') order by key settings min_rows_for_wide_part=2, fsync_after_insert=1;
insert into rep_fsync_r1 values (1);
system sync replica rep_fsync_r2;
select * from rep_fsync_r2;
optimize table rep_fsync_r1 final;
system sync replica rep_fsync_r2;
drop table rep_fsync_r1;
drop table rep_fsync_r2;

select 'compact fsync_after_insert,fsync_part_directory';
create table rep_fsync_r1 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r1') order by key settings min_rows_for_wide_part=2, fsync_after_insert=1, fsync_part_directory=1;
create table rep_fsync_r2 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r2') order by key settings min_rows_for_wide_part=2, fsync_after_insert=1, fsync_part_directory=1;
insert into rep_fsync_r1 values (1);
system sync replica rep_fsync_r2;
select * from rep_fsync_r2;
optimize table rep_fsync_r1 final;
system sync replica rep_fsync_r2;
drop table rep_fsync_r1;
drop table rep_fsync_r2;

select 'wide fsync_after_insert';
create table rep_fsync_r1 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r1') order by key settings min_bytes_for_wide_part=0, fsync_after_insert=1;
create table rep_fsync_r2 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r2') order by key settings min_bytes_for_wide_part=0, fsync_after_insert=1;
insert into rep_fsync_r1 values (1);
system sync replica rep_fsync_r2;
select * from rep_fsync_r2;
optimize table rep_fsync_r1 final;
system sync replica rep_fsync_r2;
drop table rep_fsync_r1;
drop table rep_fsync_r2;

select 'wide fsync_after_insert,fsync_part_directory';
create table rep_fsync_r1 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r1') order by key settings min_bytes_for_wide_part=0, fsync_after_insert=1, fsync_part_directory=1;
create table rep_fsync_r2 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r2') order by key settings min_bytes_for_wide_part=0, fsync_after_insert=1, fsync_part_directory=1;
insert into rep_fsync_r1 values (1);
system sync replica rep_fsync_r2;
select * from rep_fsync_r2;
optimize table rep_fsync_r1 final;
system sync replica rep_fsync_r2;
drop table rep_fsync_r1;
drop table rep_fsync_r2;

select 'wide fsync_part_directory,vertical';
create table rep_fsync_r1 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r1') order by key settings min_bytes_for_wide_part=0, fsync_part_directory=1, enable_vertical_merge_algorithm=1, vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0;
create table rep_fsync_r2 (key Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/rep_fsync', 'r2') order by key settings min_bytes_for_wide_part=0, fsync_part_directory=1, enable_vertical_merge_algorithm=1, vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0;
insert into rep_fsync_r1 values (1);
insert into rep_fsync_r2 values (2);
system sync replica rep_fsync_r2;
select * from rep_fsync_r2 order by key;
-- vertical merge does not supports deduplicate, hence no FINAL
optimize table rep_fsync_r1;
system sync replica rep_fsync_r2;
drop table rep_fsync_r1;
drop table rep_fsync_r2;
