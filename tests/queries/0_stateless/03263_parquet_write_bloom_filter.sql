-- Tags: no-fasttest, no-parallel

set engine_file_truncate_on_insert=1;
set output_format_parquet_use_custom_encoder = 1;
set output_format_parquet_row_group_size = 100;
set input_format_parquet_filter_push_down = 0;
set input_format_parquet_page_filter_push_down = 0;
set input_format_parquet_bloom_filter_push_down = 1;
set schema_inference_make_columns_nullable = 'auto';
SET enable_analyzer = 1; -- required for multiple array joins
set max_block_size = 1000000; -- have only one block to make sure rows are split into row groups deterministically
set preferred_block_size_bytes = 1000000000000;

create table data (n UInt64, s String, l UInt256, d Decimal128(4), lc LowCardinality(String)) engine Memory;
insert into data select number*100, toString(number*10), number*10000000000000000000000000000000000000000::UInt256, number*123.456::Decimal128(4), 'lc'||intDiv(number, 123)*2 from numbers(1000);

-- Baseline test without bloom filter.
insert into function file(bf_03263.parquet) select * from data settings output_format_parquet_write_bloom_filter=0;
select 'no bf: metadata', rg.num_rows, col.name, col.bloom_filter_bytes from file(bf_03263.parquet, ParquetMetadata) array join row_groups as rg array join rg.columns as col order by rg.file_offset, col.name;
select 'no bf: UInt64 hit', count(), sum(n = 12300 as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'no bf: UInt64 miss', count(), sum(n = 12345 as cond) from file(bf_03263.parquet) where indexHint(cond);

-- With bloom filter.
insert into function file(bf_03263.parquet) select * from data settings output_format_parquet_write_bloom_filter=1;
select 'bf: metadata', rg.num_rows, col.name, col.bloom_filter_bytes from file(bf_03263.parquet, ParquetMetadata) array join row_groups as rg array join rg.columns as col order by rg.file_offset, col.name;
select 'bf: UInt64 hit', count(), sum(n = 12300 as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'bf: UInt64 miss', count(), sum(n = 12345 as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'bf: String hit', count(), sum(s = '1230' as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'bf: String miss', count(), sum(s = '1234' as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'bf: UInt256 hit', count(), sum(l = 7890000000000000000000000000000000000000000::UInt256 as cond) from file(bf_03263.parquet, Parquet, 'l UInt256') where indexHint(cond);
select 'bf: UInt256 miss', count(), sum(l = 7890000000000000000000000000000000000000001::UInt256 as cond) from file(bf_03263.parquet, Parquet, 'l UInt256') where indexHint(cond);
select 'bf: Decimal128(4) hit', count(), sum(d = 108147.456::Decimal128(4) as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'bf: Decimal128(4) miss', count(), sum(d = 108147.4567::Decimal128(4) as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'bf: LowCardinality(String) hit', count(), sum(lc = 'lc4' as cond) from file(bf_03263.parquet) where indexHint(cond);
select 'bf: LowCardinality(String) miss', count(), sum(lc = 'lc1' as cond) from file(bf_03263.parquet) where indexHint(cond);

-- Try different output_format_parquet_bloom_filter_bits_per_value, check that size changed.
insert into function file(bf_03263.parquet) select * from data settings output_format_parquet_write_bloom_filter=1, output_format_parquet_bloom_filter_bits_per_value=30;
select 'more bits per value: metadata', rg.num_rows, col.name, col.bloom_filter_bytes from file(bf_03263.parquet, ParquetMetadata) array join row_groups as rg array join rg.columns as col order by rg.file_offset, col.name;
