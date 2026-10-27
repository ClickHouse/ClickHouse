-- Tags: no-fasttest

-- Parquet `TIME` is decoded as `Time64`, so its min/max stats can be used for row group and page
-- pruning. Parquet `TIMESTAMP` read with a `Time64` type hint is decoded as `DateTime64` and cast
-- to `Time64` at runtime, and that cast wraps by day, so it is not order-preserving: a row group
-- spanning midnight has raw bounds [23:00:00, 25:00:00] while its values are 23:00:00 and
-- 01:00:00. Stats must not be used in that case, otherwise matching rows get pruned away.

set engine_file_truncate_on_insert = 1;
set input_format_parquet_filter_push_down = 1;
set max_threads = 1;
set max_insert_threads = 1;

-- One row group with two timestamps straddling midnight.
insert into function file(currentDatabase() || '_05059_ts.parquet', Parquet, 't DateTime64(3, \'UTC\')')
    select arrayJoin([toDateTime64('1970-01-01 23:00:00', 3, 'UTC'), toDateTime64('1970-01-02 01:00:00', 3, 'UTC')]);

-- Both rows are visible as times of day, in both directions of the wrap.
select toString(t) from file(currentDatabase() || '_05059_ts.parquet', Parquet, 't Time64(3)') order by t;

-- The value that falls outside the raw stats range must still be found, with and without pushdown.
select count() from file(currentDatabase() || '_05059_ts.parquet', Parquet, 't Time64(3)')
    where t = toTime64('01:00:00', 3) settings log_comment = '05059prune_ts_to_time64';
select count() from file(currentDatabase() || '_05059_ts.parquet', Parquet, 't Time64(3)')
    where t = toTime64('01:00:00', 3) settings input_format_parquet_filter_push_down = 0;

system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05059prune%'
    order by log_comment;
