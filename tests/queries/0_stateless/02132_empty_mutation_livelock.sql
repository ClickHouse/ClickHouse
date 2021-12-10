drop table if exists a8x;

set empty_result_for_aggregation_by_empty_set=1;
create table a8x ENGINE = MergeTree ORDER BY tuple() settings min_bytes_for_wide_part=0 as SELECT number FROM system.numbers limit 100;

select count() from a8x;

set mutations_sync=1;
alter table a8x update number=0 WHERE number=-3;

select count() from a8x;
drop table if exists a8x;
