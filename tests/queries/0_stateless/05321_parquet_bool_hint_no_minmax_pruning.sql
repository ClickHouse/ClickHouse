-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- A Parquet integer column holding 2, read with a `LowCardinality(Bool)` hint: every nonzero value is
-- read as `true`, but the row group and page min/max statistics still say [2, 2], so pruning by them
-- dropped the rows equal to `true`. The bloom and dictionary filters lose these rows through a separate
-- mechanism, so they are pinned off to leave only the min/max legs under test.

set engine_file_truncate_on_insert = 1;
set max_threads = 1;
set max_insert_threads = 1;
set max_block_size = 1000000;
set allow_suspicious_low_cardinality_types = 1;

-- Two row groups, [0, 0] and [2, 2].
insert into function file(currentDatabase() || '_05321_rg.parquet', Parquet, 'x UInt8')
    select if(number < 1000, 0, 2) from numbers(2000) settings output_format_parquet_row_group_size = 1000;
-- The same values in one row group of many pages.
insert into function file(currentDatabase() || '_05321_pg.parquet', Parquet, 'x UInt8')
    select if(number < 1000, 0, 2) from numbers(2000)
    settings output_format_parquet_row_group_size = 1000000, output_format_parquet_data_page_size = 256,
             output_format_parquet_batch_size = 100;
insert into function file(currentDatabase() || '_05321_tp.parquet', Parquet, 'x Tuple(b UInt8)')
    select tuple(toUInt8(2)) from numbers(10);
-- A BOOLEAN column, two row groups [false, false] and [true, true].
insert into function file(currentDatabase() || '_05321_bo.parquet', Parquet, 'x Bool')
    select number >= 1000 from numbers(2000) settings output_format_parquet_row_group_size = 1000;

-- With every filter off, 1000 rows are `true`.
select count() from file(currentDatabase() || '_05321_rg.parquet', Parquet, 'x LowCardinality(Bool)') where x = true
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Row group statistics only.
select count() from file(currentDatabase() || '_05321_rg.parquet', Parquet, 'x LowCardinality(Bool)') where x = true
    settings input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05321_rg.parquet', Parquet, 'x LowCardinality(Nullable(Bool))') where x = true
    settings input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05321_rg.parquet', Parquet, 'x LowCardinality(Bool)') where x <= true
    settings input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05321_tp.parquet', Parquet, 'x Tuple(b LowCardinality(Bool))') where x.b = true
    settings input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Page statistics only.
select count() from file(currentDatabase() || '_05321_pg.parquet', Parquet, 'x LowCardinality(Bool)') where x = true
    settings log_comment = '05321page_bool', input_format_parquet_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
-- Control: the same file read as UInt8 prunes pages.
select count() from file(currentDatabase() || '_05321_pg.parquet', Parquet, 'x UInt8') where x = 2
    settings log_comment = '05321page_u8', input_format_parquet_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Controls: pruning stays for a BOOLEAN column read as `LowCardinality(Bool)`, and for the integer column
-- read as its own type.
select count() from file(currentDatabase() || '_05321_bo.parquet', Parquet, 'x LowCardinality(Bool)') where x = true
    settings log_comment = '05321prune_bool', input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05321_rg.parquet', Parquet, 'x UInt8') where x = 2
    settings log_comment = '05321prune_u8', input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05321prune%'
    order by log_comment;
select distinct log_comment, ProfileEvents['ParquetReadPages'] > 0, ProfileEvents['ParquetPrunedPages'] > 0
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05321page%'
    order by log_comment;
