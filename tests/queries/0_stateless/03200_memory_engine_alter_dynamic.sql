set allow_experimental_dynamic_type=1;
create table test (d Dynamic) engine=Memory;
insert into table test select * from numbers(5);
alter table test modify column d Dynamic(max_types=1);
select d.UInt64 from test settings enable_analyzer=1;
select d.UInt64 from test settings enable_analyzer=0;
