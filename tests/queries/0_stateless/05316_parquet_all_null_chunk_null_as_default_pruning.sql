-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- An all-NULL row group or page of a Nullable Parquet column read as non-Nullable under
-- `input_format_null_as_default` holds only the default value: min/max pruning must keep it for a
-- predicate on the default and may skip it for any other value.

set engine_file_truncate_on_insert = 1;
set input_format_null_as_default = 1;
set input_format_parquet_dictionary_filter_push_down = 0;
set input_format_parquet_bloom_filter_push_down = 0;

insert into function file(currentDatabase() || '_all_null.parquet', Parquet)
    select number, if(number < 100, NULL, number) as x from numbers(200)
    settings output_format_parquet_row_group_size = 100, max_block_size = 1000;

set input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 0;
select count(), sum(number)
    from file(currentDatabase() || '_all_null.parquet', Parquet, 'number UInt64, x UInt64') where indexHint(x = 0);
select count(), sum(number)
    from file(currentDatabase() || '_all_null.parquet', Parquet, 'number UInt64, x UInt64') where indexHint(x = 150);
select count(), sum(number)
    from file(currentDatabase() || '_all_null.parquet', Parquet, 'number UInt64, x UInt64') where x = 0;
select count(), sum(number)
    from file(currentDatabase() || '_all_null.parquet', Parquet, 'number UInt64, x Nullable(UInt64)') where indexHint(x = 0);

set input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 1;
select count(), sum(number)
    from file(currentDatabase() || '_all_null.parquet', Parquet, 'number UInt64, x UInt64') where indexHint(x = 0);
select count(), sum(number)
    from file(currentDatabase() || '_all_null.parquet', Parquet, 'number UInt64, x UInt64') where indexHint(x = 150);
select count(), sum(number)
    from file(currentDatabase() || '_all_null.parquet', Parquet, 'number UInt64, x UInt64') where x = 0;
