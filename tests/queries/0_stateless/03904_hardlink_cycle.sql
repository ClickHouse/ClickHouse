-- Tags: no-fasttest

CREATE TABLE hardlink_cycle(a UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY a settings disk = 's3_disk', min_bytes_for_wide_part=1, min_rows_for_wide_part=1, min_level_for_wide_part=0, sleep_before_loading_outdated_parts_ms=10000;

insert into hardlink_cycle select rand(), rand() from numbers(1) settings max_block_size=1, min_insert_block_size_bytes=1;

set mutations_sync=2, alter_sync=2;
alter table hardlink_cycle rename column b to c;
alter table hardlink_cycle rename column c to d;
alter table hardlink_cycle rename column d to e;

detach table hardlink_cycle;
attach table hardlink_cycle;

select 'Trying to drop table';
drop table hardlink_cycle sync;
select 'Table successfully dropped';
