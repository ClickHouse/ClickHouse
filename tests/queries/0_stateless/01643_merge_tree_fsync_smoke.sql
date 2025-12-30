-- Tags: no-object-storage, no-fasttest
-- no-fasttest: It can be slow

drop table if exists data_01643;

select 'default';
create table data_01643 (key Int) engine=MergeTree() order by key;
insert into data_01643 values (1);
select * from data_01643;
optimize table data_01643 final;
drop table data_01643;

select 'compact fsync_after_insert';
create table data_01643 (key Int) engine=MergeTree() order by key settings min_rows_for_wide_part=2, fsync_after_insert=1;
insert into data_01643 values (1);
select * from data_01643;
optimize table data_01643 final;
drop table data_01643;

select 'compact fsync_after_insert,fsync_part_directory';
create table data_01643 (key Int) engine=MergeTree() order by key settings min_rows_for_wide_part=2, fsync_after_insert=1, fsync_part_directory=1;
insert into data_01643 values (1);
select * from data_01643;
optimize table data_01643 final;
drop table data_01643;

select 'wide fsync_after_insert';
create table data_01643 (key Int) engine=MergeTree() order by key settings min_bytes_for_wide_part=0, fsync_after_insert=1;
insert into data_01643 values (1);
select * from data_01643;
optimize table data_01643 final;
drop table data_01643;

select 'wide fsync_after_insert,fsync_part_directory';
create table data_01643 (key Int) engine=MergeTree() order by key settings min_bytes_for_wide_part=0, fsync_after_insert=1, fsync_part_directory=1;
insert into data_01643 values (1);
select * from data_01643;
optimize table data_01643 final;
drop table data_01643;

select 'wide fsync_part_directory,vertical';
create table data_01643 (key Int) engine=MergeTree() order by key settings min_bytes_for_wide_part=0, fsync_part_directory=1, enable_vertical_merge_algorithm=1, vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0;
insert into data_01643 values (1);
insert into data_01643 values (2);
select * from data_01643 order by key;
-- vertical merge does not supports deduplicate, hence no FINAL
optimize table data_01643;
drop table data_01643;
