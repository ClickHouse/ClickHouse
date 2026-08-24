-- Tags: no-fasttest

-- When the requested column type doesn't exactly match the parquet type, min/max-stats-based
-- row group pruning must still work (and stay consistent with how the values themselves are
-- converted). E.g. timestamps in milliseconds read as DateTime64(6), or Float64 read as Float32.
-- https://github.com/ClickHouse/ClickHouse/issues/92659

set engine_file_truncate_on_insert = 1;
set input_format_parquet_filter_push_down = 1;
set max_threads = 1;
set max_insert_threads = 1;

-- Timestamps: one file with millisecond precision, one with microsecond, 2 row groups each,
-- in steps of 0.25 s. The files cover disjoint time ranges: January and February.
insert into function file(currentDatabase() || '_04545_ms.parquet', Parquet, 't DateTime64(3)')
    select fromUnixTimestamp64Milli(toUnixTimestamp(toDateTime('2024-01-01 00:00:00', 'UTC')) * 1000 + number * 250, 'UTC') from numbers(2000)
    settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_04545_us.parquet', Parquet, 't DateTime64(6)')
    select fromUnixTimestamp64Micro(toUnixTimestamp(toDateTime('2024-02-01 00:00:00', 'UTC')) * 1000000 + number * 250000, 'UTC') from numbers(2000)
    settings output_format_parquet_row_group_size = 1000;

-- Read with both scales; all row groups are in 2024, so nothing matches and all 4 must be pruned.
select count() from file(currentDatabase() || '_04545_{ms,us}.parquet', Parquet, 't DateTime64(3, \'UTC\')')
    where t >= toDateTime64('2025-01-01 00:00:00', 3, 'UTC') settings log_comment = '04545prune_dt64_a3';
select count() from file(currentDatabase() || '_04545_{ms,us}.parquet', Parquet, 't DateTime64(6, \'UTC\')')
    where t >= toDateTime64('2025-01-01 00:00:00', 6, 'UTC') settings log_comment = '04545prune_dt64_a6';

-- Only the microsecond (February) file matches; the millisecond file's 2 row groups must be pruned.
select count() from file(currentDatabase() || '_04545_{ms,us}.parquet', Parquet, 't DateTime64(3, \'UTC\')')
    where t >= toDateTime64('2024-02-01 00:00:00', 3, 'UTC') settings log_comment = '04545prune_dt64_b3';
select count() from file(currentDatabase() || '_04545_{ms,us}.parquet', Parquet, 't DateTime64(6, \'UTC\')')
    where t >= toDateTime64('2024-02-01 00:00:00', 6, 'UTC') settings log_comment = '04545prune_dt64_b6';

-- Cut inside each file: prune the first row group of each file (its max is +249.75s), keep the
-- second one (rows 1000..1999, i.e. +250s..+499.75s).
select count() from file(currentDatabase() || '_04545_ms.parquet', Parquet, 't DateTime64(6, \'UTC\')')
    where t > toDateTime64('2024-01-01 00:04:15', 6, 'UTC') settings log_comment = '04545prune_dt64_c6';
select count() from file(currentDatabase() || '_04545_us.parquet', Parquet, 't DateTime64(3, \'UTC\')')
    where t > toDateTime64('2024-02-01 00:04:15', 3, 'UTC') settings log_comment = '04545prune_dt64_c3';

-- Truncation consistency around the epoch: reading microseconds as DateTime64(3) truncates values
-- toward zero, for stats and values alike. Both -375us and +375us become 0.000, so `t > 0.000`
-- matches nothing (and prunes the row group), while `t >= 0.000` matches both rows.
insert into function file(currentDatabase() || '_04545_epoch.parquet', Parquet, 't DateTime64(6)')
    select arrayJoin([toDateTime64(-0.000375, 6, 'UTC'), toDateTime64(0.000375, 6, 'UTC')]);
select count() from file(currentDatabase() || '_04545_epoch.parquet', Parquet, 't DateTime64(3, \'UTC\')')
    where t > toDateTime64(0, 3, 'UTC') settings log_comment = '04545prune_dt64_trunc_gt';
select count() from file(currentDatabase() || '_04545_epoch.parquet', Parquet, 't DateTime64(3, \'UTC\')')
    where t >= toDateTime64(0, 3, 'UTC') settings log_comment = '04545prune_dt64_trunc_ge';

-- Floats: FLOAT file read as Float64 and DOUBLE file read as Float32.
insert into function file(currentDatabase() || '_04545_f32.parquet', Parquet, 'x Float32')
    select number from numbers(2000) settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_04545_f64.parquet', Parquet, 'x Float64')
    select 10000 + number from numbers(2000) settings output_format_parquet_row_group_size = 1000;

select count() from file(currentDatabase() || '_04545_f32.parquet', Parquet, 'x Float64')
    where x > 1500. settings log_comment = '04545prune_float_widen';
select count() from file(currentDatabase() || '_04545_f64.parquet', Parquet, 'x Float32')
    where x > 11500. settings log_comment = '04545prune_float_narrow';

-- A Float64 value that doesn't fit in Float32: the stats bound can't be converted, so the row
-- group is read (not pruned), and the value itself becomes +inf.
insert into function file(currentDatabase() || '_04545_f64_big.parquet', Parquet, 'x Float64')
    select 1e300;
select count() from file(currentDatabase() || '_04545_f64_big.parquet', Parquet, 'x Float32')
    where x > 1000. settings log_comment = '04545prune_float_overflow';

-- Decimals: DECIMAL(18, 3) file read with different scales and sizes.
-- (The filter constants are spelled with explicit Decimal types because a plain `1500.5` literal
-- is a Float64, and KeyCondition doesn't produce ranges for a Float constant compared with a
-- Decimal column - a separate pre-existing limitation.)
insert into function file(currentDatabase() || '_04545_dec.parquet', Parquet, 'd Decimal64(3)')
    select toDecimal64(number, 3) from numbers(2000) settings output_format_parquet_row_group_size = 1000;

select count() from file(currentDatabase() || '_04545_dec.parquet', Parquet, 'd Decimal64(6)')
    where d > toDecimal64(1500.5, 6) settings log_comment = '04545prune_dec_upscale';
select count() from file(currentDatabase() || '_04545_dec.parquet', Parquet, 'd Decimal64(1)')
    where d > toDecimal64(1500.5, 1) settings log_comment = '04545prune_dec_downscale';
select count() from file(currentDatabase() || '_04545_dec.parquet', Parquet, 'd Decimal32(3)')
    where d > toDecimal32(1500.5, 3) settings log_comment = '04545prune_dec_narrow';
select count() from file(currentDatabase() || '_04545_dec.parquet', Parquet, 'd Decimal128(20)')
    where d > toDecimal128(1500.5, 20) settings log_comment = '04545prune_dec_widen';

-- Check that row groups were actually pruned (or not, in the float overflow case).
-- (distinct because the test may run multiple times in the same database, and the counters are
-- deterministic.)
system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '04545prune%'
    order by log_comment;
