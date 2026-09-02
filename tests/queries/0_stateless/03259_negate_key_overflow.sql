create table a (x UInt64) engine MergeTree order by x;
insert into a values (12345678901234567890), (42);
select * from a where -x = -42;
drop table a;

create table a (x UInt128) engine MergeTree order by x;
insert into a values (170141183460469231731687303715884105828), (42);
select * from a where -x = -42;
drop table a;

create table a (x UInt256) engine MergeTree order by x;
insert into a values (57896044618658097711785492504343953926634992332820282019728792003956564820068), (42);
select * from a where -x = -42;
drop table a;
