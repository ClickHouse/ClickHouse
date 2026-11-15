drop table if exists test;
create table test (a Array(UInt64), `a.size0` UInt64) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1;
insert into test select [number], number from numbers(10);
select a.size0 from test;
select a, a.size0 from test;
drop table test;

