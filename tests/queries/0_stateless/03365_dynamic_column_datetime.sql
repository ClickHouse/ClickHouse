set enable_dynamic_type = 1;

drop table if exists test;
create table test (d Dynamic) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
insert into test select toDateTime64(materialize('2024-01-01'), 3, 'Asia/Istanbul');

drop table test;