drop table if exists test1;
drop table if exists test2;
drop table if exists test_merge;

create table test1 (x UInt64, t Tuple(a UInt32, b UInt32), y String) engine=Memory;
create table test2 (x UInt64, t Tuple(a UInt32, b UInt32), y String) engine=Memory;
create table test_merge (x UInt64, t Tuple(a UInt32, b UInt32), y String) engine=Merge(currentDatabase(), 'test');

insert into test1 select 1, tuple(2, 3), 's1';
insert into test2 select 4, tuple(5, 6), 's2';

set allow_suspicious_types_in_order_by=1;

select * from test_merge order by all;
select t.a from test_merge order by all;
select t.b from test_merge order by all;
select x, t.a from test_merge order by all;
select y, t.a from test_merge order by all;
select t.a, t.b from test_merge order by all;
select x, t.a, t.b from test_merge order by all;
select y, t.a, t.b from test_merge order by all;
select x, t.a, t.b, y from test_merge order by all;

drop table test_merge;
drop table test1;
drop table test2;

drop table if exists test;
create table test (json JSON) engine=Memory;
create table test_merge (json JSON) engine=Merge(currentDatabase(), 'test');
insert into test values ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');

select json.a.b, json.a.g, json.c, json.d from test_merge;
drop table test_merge;
drop table test;

