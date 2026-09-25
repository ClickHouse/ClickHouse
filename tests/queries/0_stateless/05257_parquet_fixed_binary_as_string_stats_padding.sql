-- Tags: no-fasttest

-- A FixedString(N) column is stored in Parquet as a fixed length byte array, so a value shorter than
-- N is padded with trailing zero bytes. Reading that column back as String removes the padding, so a
-- row holding 'ab' in a 4 byte column reads as 'ab'. The min/max statistics of the row group and of
-- the page index keep the padded bytes, and a predicate constant compared against them fell outside
-- the stored range, so an equality filter silently returned no rows at all.
-- Found while reviewing https://github.com/ClickHouse/ClickHouse/pull/121077

set engine_file_truncate_on_insert = 1;
set max_threads = 1;
set max_insert_threads = 1;

-- The bloom filter and the dictionary page match on a hash of the value rather than on its order.
-- They are a separate mechanism with a separate defect, and this test is about the min/max statistics.
set input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- 'ab' padded to 4 bytes next to a value that fills the width, so the stored minimum is padded.
insert into function file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x FixedString(4)')
    select toFixedString(x, 4) from values('x String', 'ab', 'abcd');

-- An all zero value, which reads back as the empty string.
insert into function file(currentDatabase() || '_05257_zero.parquet', Parquet, 'x FixedString(4)')
    select toFixedString(x, 4) from values('x String', '', 'abcd');

-- 'a' and 'a\0b': the stored maximum carries a zero byte in the middle, which must survive while the
-- trailing ones are removed.
insert into function file(currentDatabase() || '_05257_interior.parquet', Parquet, 'x FixedString(4)')
    select toFixedString(unhex(x), 4) from values('x String', '61', '610062');

-- Two values that are both shorter than the width, so the stored maximum carries padding too.
insert into function file(currentDatabase() || '_05257_padmax.parquet', Parquet, 'x FixedString(4)')
    select toFixedString(x, 4) from values('x String', 'aa', 'ab');

-- The stored statistics really do carry the padding (rendered here as decimal bytes).
select trimBoth(row_groups[1].columns[1].statistics.min), trimBoth(row_groups[1].columns[1].statistics.max)
    from file(currentDatabase() || '_05257_pad.parquet', ParquetMetadata);

-- The padded value is in the file, so all three must return 1: the row group statistics leg (the
-- default), the page index leg on its own, and both legs off.
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String')
    where x = 'ab';
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String')
    where x = 'ab' settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String')
    where x = 'ab' settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- An all zero value must be matchable as the empty string.
select count() from file(currentDatabase() || '_05257_zero.parquet', Parquet, 'x String')
    where x = '';
select count() from file(currentDatabase() || '_05257_zero.parquet', Parquet, 'x String')
    where x = '' settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05257_zero.parquet', Parquet, 'x String')
    where x = '' settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- The same read through a nullable and through a low cardinality output type.
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x Nullable(String)')
    where x = 'ab';
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x Nullable(String)')
    where x = 'ab' settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x Nullable(String)')
    where x = 'ab' settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x LowCardinality(String)')
    where x = 'ab';
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x LowCardinality(String)')
    where x = 'ab' settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x LowCardinality(String)')
    where x = 'ab' settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- A value whose own zero byte is not at the end stays matchable, so only trailing zeros are removed.
select count() from file(currentDatabase() || '_05257_interior.parquet', Parquet, 'x String')
    where x = unhex('610062');
select count() from file(currentDatabase() || '_05257_interior.parquet', Parquet, 'x String')
    where x = unhex('610062') settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05257_interior.parquet', Parquet, 'x String')
    where x = unhex('610062') settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- The padded minimum of this fixture must match, exactly like the first one.
select count() from file(currentDatabase() || '_05257_padmax.parquet', Parquet, 'x String')
    where x = 'aa';
select count() from file(currentDatabase() || '_05257_padmax.parquet', Parquet, 'x String')
    where x = 'aa' settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05257_padmax.parquet', Parquet, 'x String')
    where x = 'aa' settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- Reads that were never affected: a value that fills the width, the same column read back as
-- FixedString(4), and an unfiltered count.
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String')
    where x = 'abcd';
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x FixedString(4)')
    where x = toFixedString('ab', 4);
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String');

-- Values outside the column's range must still be pruned, above the maximum and below the minimum,
-- so that matching the padded rows does not cost the pruning itself.
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String')
    where x = 'zz' settings log_comment = '05257prune_above';
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String')
    where x = '' settings log_comment = '05257prune_below';

-- The page index leg must be armed and not merely non-pruning: with the row group leg switched off,
-- a constant above the maximum has to be ruled out at the page level.
select count() from file(currentDatabase() || '_05257_pad.parquet', Parquet, 'x String')
    where x = 'zz' settings input_format_parquet_filter_push_down = 0, log_comment = '05257prune_page';

-- The maximum must be stripped as well as the minimum: the values read as 'aa' and 'ab', so nothing
-- is above 'ab' and the row group has to be pruned. An unstripped maximum of 'ab\0\0' would sort
-- above the constant and the row group would be read instead.
select count() from file(currentDatabase() || '_05257_padmax.parquet', Parquet, 'x String')
    where x > 'ab' settings log_comment = '05257prune_maxside';

system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups'],
       ProfileEvents['ParquetReadPages'], ProfileEvents['ParquetPrunedPages']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05257prune%'
    order by log_comment;
