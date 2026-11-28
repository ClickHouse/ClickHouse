create table test (x UInt64, d Variant(UInt64)) engine=Memory;
insert into test select number, null from numbers(200000);
select d from test order by d::String limit 32213 format Null;
drop table test;

