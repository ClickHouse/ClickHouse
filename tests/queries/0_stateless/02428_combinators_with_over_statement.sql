drop table if exists test;
create table test (x AggregateFunction(uniq, UInt64), y Int64) engine=Memory;
set max_insert_threads = 1;
insert into test select uniqState(number) as x, number as y from numbers(10) group by number order by y;
select uniqStateMap(map(1, x)) OVER (PARTITION BY y) from test;
select uniqStateForEach([x]) OVER (PARTITION BY y) from test;
select uniqStateResample(30, 75, 30)([x], 30) OVER (PARTITION BY y) from test;
select uniqStateForEachMapForEach([map(1, [x])]) OVER (PARTITION BY y) from test;
select uniqStateDistinctMap(map(1, x)) OVER (PARTITION BY y) from test;
drop table test;

