-- It is special because actions cannot be reused for SimpleAggregateFunction (see https://github.com/ClickHouse/ClickHouse/pull/54436)
SET allow_suspicious_primary_key = 1;
drop table if exists data;
create table data (key Int) engine=AggregatingMergeTree() order by tuple();
insert into data values (0);
select * from data final prewhere indexHint(_partition_id = 'all') or indexHint(_partition_id = 'all');
select * from data final prewhere indexHint(_partition_id = 'all') or indexHint(_partition_id = 'all') or indexHint(_partition_id = 'all');
