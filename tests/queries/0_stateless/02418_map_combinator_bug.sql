drop table if exists test;
create table test (x Map(UInt8, AggregateFunction(uniq, UInt64))) engine=Memory;
insert into test select uniqStateMap(map(1, number)) from numbers(10);
select * from test format Null;
drop table test;

create table test (x AggregateFunction(uniq, UInt64), y Int64) engine=Memory;
insert into test select uniqState(number) as x, number as y from numbers(10) group by number;
select uniqStateMap(map(1, x)) OVER (PARTITION BY y) from test; -- {serverError ILLEGAL_COLUMN}
drop table test;

