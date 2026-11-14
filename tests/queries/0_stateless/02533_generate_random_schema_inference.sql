drop table if exists test;
create table test (x UInt32, y String) engine=Memory;
insert into test select * from generateRandom() limit 10;
select count() from test;
drop table test;

