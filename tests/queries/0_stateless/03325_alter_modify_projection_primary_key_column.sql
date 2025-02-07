drop table if exists test;
create table test (x UInt16, y UInt16, projection proj (select * order by x)) engine=MergeTree order by tuple();
insert into test select number, number from numbers(1000000);
alter table test modify column x UInt64;
select * from test where x = 10;
drop table test;

