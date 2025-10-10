set query_plan_optimize_lazy_materialization=1;
set query_plan_max_limit_for_lazy_materialization=10;

drop table if exists test;
create table test (x UInt64, y UInt64, a Array(UInt64)) engine=MergeTree order by x settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
insert into test select number, number, range(number) from numbers(10);
select a.size0, a from test where y > 5 order by y limit 2;
select a, a.size0 from test where y > 5 order by y limit 2;

