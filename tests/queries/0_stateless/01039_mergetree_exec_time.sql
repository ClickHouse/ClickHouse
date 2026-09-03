DROP TABLE IF EXISTS tab;
create table tab (A Int64) Engine=MergeTree order by tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
insert into tab select cityHash64(number) from numbers(1000);
select sum(sleep(0.1)) from tab settings max_block_size = 1, max_execution_time=1; -- { serverError TIMEOUT_EXCEEDED }
DROP TABLE IF EXISTS tab;
