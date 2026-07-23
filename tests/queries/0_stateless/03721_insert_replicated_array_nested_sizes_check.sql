set enable_analyzer=1;

drop table if exists test;
create table test (a Array(UInt64)) engine=MergeTree order by tuple();
insert into test select range(number) from numbers(3) array join range(number + 1);
select * from test;
drop table test;
