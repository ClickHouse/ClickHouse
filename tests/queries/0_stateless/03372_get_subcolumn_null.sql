drop table if exists test;
create table test (x Nullable(UInt32)) engine=Memory;
insert into test select number % 2 ? null : number from numbers(10);
select getSubcolumn(x, 'null') from test;
drop table test;

