drop table if exists test_lazy;
create table test_lazy (id UInt64) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
insert into test_lazy select * from numbers(100);
alter table test_lazy add column array Array(UInt64) settings mutations_sync=1;
select id, array from test_lazy where id = 42 order by id limit 10 settings query_plan_optimize_lazy_materialization = 1;
drop table test_lazy;

