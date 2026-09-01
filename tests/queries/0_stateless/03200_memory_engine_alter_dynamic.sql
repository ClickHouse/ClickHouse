set allow_experimental_dynamic_type=1;
drop table if exists test;
create table test (d Dynamic) engine=Memory;
insert into table test select * from numbers(5);
alter table test modify column d Dynamic(max_types=0);
select d.UInt64 from test settings enable_analyzer=1;
select d.UInt64 from test settings enable_analyzer=0;
drop table test;
