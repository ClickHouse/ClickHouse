drop table if exists test.tab;
create table test.tab (date Date, val UInt64) engine = MergeTree(date, (date, val), 8192);
insert into remote('localhost', test.tab)  values ('2017-11-03', 42);
select * from test.tab;

