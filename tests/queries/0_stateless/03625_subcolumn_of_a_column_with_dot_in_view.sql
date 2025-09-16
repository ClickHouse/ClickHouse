set enable_analyzer=1;

drop table if exists test;
drop view if exists v;
create table test (`t.t` Tuple(a UInt32)) engine=Memory;
create view v as select * from test;
insert into test select tuple(42);
select t.t.a from v;
drop view v;
drop table test;

