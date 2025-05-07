drop table if exists test;
create table test (a UInt32, d Dynamic, x UInt32, y UInt32, z UInt32) engine=Memory;
insert into test select 1, 94, 1, 0, 3;
insert into test select 2, 40000, 1, 10, 3;
select x, y, z, argMax(d, a), max(a), argMin(d, a), min(a) from test group by x, y, z order by x, y, z;
drop table test;

