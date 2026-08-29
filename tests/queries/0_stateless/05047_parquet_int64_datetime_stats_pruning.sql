-- Tags: no-fasttest

-- Plain INT64 parquet columns (no logical type annotation) read as DateTime support min/max
-- row group pruning. Stats outside the DateTime range [0, 4294967295] are not usable as bounds
-- (the cast saturates), so such row groups must stay unpruned.

set engine_file_truncate_on_insert = 1;
set input_format_parquet_filter_push_down = 1;
set max_threads = 1;
set max_insert_threads = 1;

-- 4 row groups of 1000 epoch-second values each, disjoint ranges. max_block_size is pinned so
-- the data arrives in a single block and row group boundaries are deterministic.
insert into function file(currentDatabase() || '_05047.parquet', Parquet, 't Int64')
    select 1000000000 + number from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, max_block_size = 1000000;

select count() from file(currentDatabase() || '_05047.parquet', Parquet, 't DateTime')
    where t <= toDateTime(1000000999, 'UTC') settings log_comment = '05047prune_first';
select count() from file(currentDatabase() || '_05047.parquet', Parquet, 't DateTime')
    where t >= toDateTime(1000004000, 'UTC') settings log_comment = '05047prune_all';
select count() from file(currentDatabase() || '_05047.parquet', Parquet, 't DateTime')
    where t >= toDateTime(1000002000, 'UTC') settings log_comment = '05047prune_half';

-- One row group per value; the first and last have stats outside the DateTime range, so only
-- the middle row group is prunable.
insert into function file(currentDatabase() || '_05047_of.parquet', Parquet, 't Int64')
    select arrayJoin([toInt64(-100), 1000000000, 5000000000])
    settings output_format_parquet_row_group_size = 1;

select count() from file(currentDatabase() || '_05047_of.parquet', Parquet, 't DateTime')
    where t <= toDateTime(0, 'UTC') settings log_comment = '05047prune_of_low';
select count() from file(currentDatabase() || '_05047_of.parquet', Parquet, 't DateTime')
    where t >= toDateTime(4294967295, 'UTC') settings log_comment = '05047prune_of_high';
select count() from file(currentDatabase() || '_05047_of.parquet', Parquet, 't DateTime')
    where t = toDateTime(1000000000, 'UTC') settings log_comment = '05047prune_of_mid';

-- Same results with pruning disabled.
select count() from file(currentDatabase() || '_05047_of.parquet', Parquet, 't DateTime')
    where t <= toDateTime(0, 'UTC') settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05047_of.parquet', Parquet, 't DateTime')
    where t >= toDateTime(4294967295, 'UTC') settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05047_of.parquet', Parquet, 't DateTime')
    where t = toDateTime(1000000000, 'UTC') settings input_format_parquet_filter_push_down = 0;

system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05047prune%'
    order by log_comment;
