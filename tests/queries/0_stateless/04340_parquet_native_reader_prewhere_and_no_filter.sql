-- Tags: no-fasttest
-- no-fasttest: needs Parquet.

-- Regression test for a LOGICAL_ERROR "Unexpected column in sample block: and(...)" in the
-- ParquetV3 native reader's preparePrewhere(). It was thrown when a multi-condition and()
-- PREWHERE was kept in the output but did not need filtering (need_filter = false), which
-- happens with the old analyzer when a query has both a PREWHERE and a WHERE.

set engine_file_truncate_on_insert = 1;

insert into function file(currentDatabase() || '/04340_parquet_native_reader_prewhere.parquet')
    select number::UInt32 as u32, number as n, toString(number) as s from numbers(1000);

set input_format_parquet_use_native_reader_v3 = 1;

-- Old analyzer: PREWHERE and(...) + WHERE leaves need_filter = false and keeps the prewhere column.
select count() from file(currentDatabase() || '/04340_parquet_native_reader_prewhere.parquet')
    prewhere and(u32 < 500, n > 2) where s != 'zzz'
    settings enable_analyzer = 0;

-- Three conditions.
select count() from file(currentDatabase() || '/04340_parquet_native_reader_prewhere.parquet')
    prewhere and(u32 < 500, n > 2, s != '1') where s != 'zzz'
    settings enable_analyzer = 0;

-- indexHint variant (as seen by the fuzzer).
select count() from file(currentDatabase() || '/04340_parquet_native_reader_prewhere.parquet')
    prewhere and(indexHint(u32 in (1, 2, 3)), n > 2) where materialize(1)
    settings enable_analyzer = 0;

-- New analyzer must keep filtering correctly (need_filter = true path).
select count() from file(currentDatabase() || '/04340_parquet_native_reader_prewhere.parquet')
    prewhere and(u32 < 500, n > 2) where s != 'zzz'
    settings enable_analyzer = 1;
