-- https://github.com/ClickHouse/ClickHouse/issues/93817
drop table if exists data_add_minmax_index_for_numeric_columns;
create table data_add_minmax_index_for_numeric_columns (key Int, value Int) engine=MergeTree() order by key settings merge_selector_base = 1000, index_granularity=8192, index_granularity_bytes=10e9, min_bytes_for_wide_part=1e9, add_minmax_index_for_numeric_columns=1 as
select *, *+1000000 from numbers(100000) settings max_insert_threads=1;
-- { echoOn }
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data_add_minmax_index_for_numeric_columns, key = 8193);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data_add_minmax_index_for_numeric_columns, key = 8193, 'no_such_part');
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data_add_minmax_index_for_numeric_columns, key = 8193, '.*|no_such_part');
-- { echoOff }
