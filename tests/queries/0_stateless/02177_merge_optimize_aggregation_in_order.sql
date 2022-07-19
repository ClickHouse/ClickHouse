drop table if exists data_02177;
create table data_02177 (key Int) Engine=MergeTree() order by key;
insert into data_02177 values (1);

set optimize_aggregation_in_order=1;

-- { echoOn }

-- regression for optimize_aggregation_in_order
-- that cause "Chunk should have AggregatedChunkInfo in GroupingAggregatedTransform" error
select count() from remote('127.{1,2}', currentDatabase(), data_02177) group by key;
select count() from remote('127.{1,2}', currentDatabase(), data_02177) group by key settings distributed_aggregation_memory_efficient=0;

-- { echoOff }
drop table data_02177;
