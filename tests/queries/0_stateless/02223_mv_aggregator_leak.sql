drop table if exists mv_02223;
drop table if exists in_02223;
drop table if exists out_02223;

create table in_02223 (key UInt64) engine=Null();
create table out_02223 (keys AggregateFunction(uniqExact, String)) engine=Null();

create materialized view mv_02223 to out_02223 as select uniqExactState(toString(key)) as keys from in_02223 group by intDiv(key, 1000);

-- Here SET is used (over SETTINGS in INSERT) to test it on 19.x
-- since the issue was introduced in 19.11.12.69
-- https://github.com/ClickHouse/ClickHouse/pull/3796
set max_memory_usage=200000000;
insert into in_02223 select * from numbers(10000000);

drop table mv_02223;
drop table out_02223;
drop table in_02223;
