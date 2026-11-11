drop table if exists test;
create table test (x UInt32, y String, d Date) engine=Memory() as select number as x, toString(number) as y, toDate(number) as d from numbers(10);
insert into table function file('data.native.zst') select * from test settings engine_file_truncate_on_insert=1;
desc file('data.native.zst');
select * from file('data.native.zst');
