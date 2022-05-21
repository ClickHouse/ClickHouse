drop table if exists data_02176;
create table data_02176 (key Int) Engine=MergeTree() order by key;

set optimize_aggregation_in_order=1;

-- { echoOn }

-- regression for optimize_aggregation_in_order with empty result set
-- that cause at first
--   "Chunk should have AggregatedChunkInfo in GroupingAggregatedTransform"
-- at first and after
--   "Chunk should have AggregatedChunkInfo in GroupingAggregatedTransform"
select count() from remote('127.{1,2}', currentDatabase(), data_02176) where key = 0 group by key;
select count() from remote('127.{1,2}', currentDatabase(), data_02176) where key = 0 group by key settings distributed_aggregation_memory_efficient=0;

-- { echoOff }
drop table data_02176;
