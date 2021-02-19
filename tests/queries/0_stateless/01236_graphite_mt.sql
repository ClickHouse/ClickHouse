drop table if exists test_graphite;
create table test_graphite (key UInt32, Path String, Time DateTime, Value Float64, Version UInt32, col UInt64) engine = GraphiteMergeTree('graphite_rollup') order by key settings index_granularity=10;

insert into test_graphite 
select 1, 'sum_1', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300) union all 
select 2, 'sum_1', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300) union all
select 1, 'sum_2', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300) union all
select 2, 'sum_2', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300) union all
select 1, 'max_1', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300) union all 
select 2, 'max_1', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300) union all
select 1, 'max_2', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300) union all
select 2, 'max_2', toDateTime(today()) - number * 60 - 30, number, 1, number from numbers(300);

insert into test_graphite 
select 1, 'sum_1', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200) union all
select 2, 'sum_1', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200) union all
select 1, 'sum_2', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200) union all
select 2, 'sum_2', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200) union all
select 1, 'max_1', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200) union all
select 2, 'max_1', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200) union all
select 1, 'max_2', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200) union all
select 2, 'max_2', toDateTime(today() - 3) - number * 60 - 30, number, 1, number from numbers(1200);

optimize table test_graphite;

select key, Path, Value, Version, col from test_graphite order by key, Path, Time desc;
