-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- A Parquet UINT_8 column read with a `Bool` hint: every nonzero value is read as `true`, so `x = true`,
-- `x IN (true)` and `WHERE x` agree, and min/max statistics above 1 bound the column as 1.

set engine_file_truncate_on_insert = 1;
set max_threads = 1;
set max_insert_threads = 1;
set max_block_size = 1000000;
set allow_suspicious_low_cardinality_types = 1;

insert into function file(currentDatabase() || '_05331_v.parquet', Parquet, 'x UInt8') select toUInt8(2) from numbers(10);
insert into function file(currentDatabase() || '_05331_plain.parquet', Parquet, 'x UInt8') select toUInt8(2) from numbers(10)
    settings output_format_parquet_max_dictionary_size = 0;
insert into function file(currentDatabase() || '_05331_n.parquet', Parquet, 'x Nullable(UInt8)') select arrayJoin([0, 2, NULL, 255]);
insert into function file(currentDatabase() || '_05331_a.parquet', Parquet, 'x Array(UInt8)') select [0, 2, 3];
insert into function file(currentDatabase() || '_05331_t.parquet', Parquet, 'x Tuple(b UInt8)') select tuple(toUInt8(2));
insert into function file(currentDatabase() || '_05331_m.parquet', Parquet, 'x Map(String, UInt8)') select map('a', toUInt8(2));
-- Two row groups, [0, 0] and [2, 2].
insert into function file(currentDatabase() || '_05331_rg.parquet', Parquet, 'x UInt8')
    select if(number < 1000, 0, 2) from numbers(2000) settings output_format_parquet_row_group_size = 1000;
-- The same values in one row group of many pages.
insert into function file(currentDatabase() || '_05331_pg.parquet', Parquet, 'x UInt8')
    select if(number < 1000, 0, 2) from numbers(2000)
    settings output_format_parquet_row_group_size = 1000000, output_format_parquet_data_page_size = 256,
             output_format_parquet_batch_size = 100;
insert into function file(currentDatabase() || '_05331_nb.parquet', Parquet, 'x UInt8') select toUInt8(2) from numbers(10)
    settings output_format_parquet_write_bloom_filter = 0;
-- A BOOLEAN column.
insert into function file(currentDatabase() || '_05331_bo.parquet', Parquet, 'x Bool') select number % 2 = 1 from numbers(10);

-- Every filter off, dictionary-encoded and PLAIN.
select any(toUInt8(x)), countIf(x = true), countIf(x in (true)), countIf(x) from file(currentDatabase() || '_05331_v.parquet', Parquet, 'x Bool')
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select any(toUInt8(x)), countIf(x = true), countIf(x in (true)), countIf(x) from file(currentDatabase() || '_05331_plain.parquet', Parquet, 'x Bool')
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Wrapped and nested.
select toUInt8(x) from file(currentDatabase() || '_05331_n.parquet', Parquet, 'x Nullable(Bool)')
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select arrayMap(v -> toUInt8(v), x) from file(currentDatabase() || '_05331_a.parquet', Parquet, 'x Array(Bool)')
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select toUInt8(x.b) from file(currentDatabase() || '_05331_t.parquet', Parquet, 'x Tuple(b Bool)')
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select toUInt8(x['a']) from file(currentDatabase() || '_05331_m.parquet', Parquet, 'x Map(String, Bool)')
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Row group statistics only: [2, 2] bounds the column as [1, 1].
select count() from file(currentDatabase() || '_05331_rg.parquet', Parquet, 'x Bool') where x = false
    settings log_comment = '05331prune_false', input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05331_rg.parquet', Parquet, 'x Bool') where x = true
    settings log_comment = '05331prune_true', input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Page statistics only.
select count() from file(currentDatabase() || '_05331_pg.parquet', Parquet, 'x Bool') where x = false
    settings log_comment = '05331page_false', input_format_parquet_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- No bloom filter in the file, and the dictionary filter on.
select count() from file(currentDatabase() || '_05331_nb.parquet', Parquet, 'x Bool') where x = true
    settings input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05331_nb.parquet', Parquet, 'x LowCardinality(Bool)') where x = true
    settings input_format_parquet_dictionary_filter_push_down = 1048576;

-- Controls: the integer column read as its own type, and a BOOLEAN column.
select countIf(x = 2), any(x) from file(currentDatabase() || '_05331_v.parquet', Parquet, 'x UInt8');
select countIf(x = true), countIf(x = false) from file(currentDatabase() || '_05331_bo.parquet', Parquet, 'x Bool');
select sum(x) from file(currentDatabase() || '_05331_bo.parquet', Parquet, 'x UInt8');

system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05331prune%'
    order by log_comment;
select distinct log_comment, ProfileEvents['ParquetReadPages'] > 0, ProfileEvents['ParquetPrunedPages'] > 0
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05331page%'
    order by log_comment;
