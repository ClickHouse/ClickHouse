drop table if exists data;

create table data (key Int, val1 SimpleAggregateFunction(max, Nullable(Int)), val2 SimpleAggregateFunction(min, Int)) engine=AggregatingMergeTree() order by key;
system stop merges data;

insert into data values (1,10,100);
insert into data values (1,20,10);

select key, val1, val2, assumeNotNull(val1) > val2 x1, val1 > val2 x2 from data final prewhere assumeNotNull(val1) > 0 where x1 != x2 settings max_threads=1;
