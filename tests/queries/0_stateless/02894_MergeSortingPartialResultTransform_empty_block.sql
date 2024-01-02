drop table if exists data;
create table data (key Int) engine=MergeTree() order by key;
insert into data select * from numbers(1);
insert into data select * from numbers(1);
system stop merges data;
-- need sleep to trigger partial results to uncover the bug with empty chunk after remerge due to empty array join, i.e.:
--
--   MergeSortingTransform: Re-merging intermediate ORDER BY data (1 blocks with 0 rows) to save memory consumption
--   MergeSortingTransform: Memory usage is lowered from 4.26 KiB to 0.00 B
--
select key, sleepEachRow(1) from data array join [] as x order by key settings optimize_read_in_order=0, allow_experimental_partial_result=1, partial_result_update_duration_ms=1, max_threads=1, max_execution_time=0, max_block_size=1;
