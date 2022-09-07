drop table if exists test;
create table test (x Map(UInt8, AggregateFunction(uniq, UInt64))) engine=Memory;
insert into test select uniqStateMap(map(1, number)) from numbers(10);
select * from test format Null;
truncate table test;
drop table test;

create table test (x Map(UInt8, Array(Map(UInt8, Array(AggregateFunction(uniq, UInt64)))))) engine=Memory;
insert into test select uniqStateForEachMapForEachMap(map(1, [map(1, [number, number])])) from numbers(10);
select * from test format Null;
truncate table test;
drop table test;

create table test (x Array(Array(AggregateFunction(uniq, UInt64)))) engine=Memory;
insert into test select uniqStateForEachResample(30, 75, 30)([number, number + 1], 30) from numbers(10);
select * from test format Null;
truncate table test;
drop table test;

create table test (x Array(Array(Map(UInt8, AggregateFunction(uniq, UInt64))))) engine=Memory;
insert into test select uniqStateMapForEachResample(30, 75, 30)([map(1, number)], 30) from numbers(10);
select * from test format Null;
truncate table test;
drop table test;

