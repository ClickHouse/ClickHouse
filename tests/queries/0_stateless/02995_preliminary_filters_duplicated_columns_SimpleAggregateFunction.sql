-- It is special because actions cannot be reused for SimpleAggregateFunction (see https://github.com/ClickHouse/ClickHouse/pull/54436)
set allow_suspicious_primary_key = 1;
drop table if exists data;
create table data (key SimpleAggregateFunction(max, Int)) engine=AggregatingMergeTree() order by tuple();
insert into data values (0);
select * from data final prewhere indexHint(_partition_id = 'all') and key >= -1 where key >= 0;
