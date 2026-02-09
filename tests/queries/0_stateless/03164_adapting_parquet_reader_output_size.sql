-- Tags: no-fasttest, no-random-settings, no-parallel

set max_insert_threads=1;
set schema_inference_make_columns_nullable=0;
set engine_file_truncate_on_insert=1;

-- Average string lengths, approximately: 2, 200, 200, 200
INSERT INTO FUNCTION file('03164_adapting_parquet_reader_output_size.parquet', Parquet, 'short String, long1 String, long2 String, long_low_cardinality String') SELECT number%100, range(cityHash64(number), cityHash64(number)+10), repeat(cityHash64(number)::String, 6+number%10), repeat((number%10)::String, 200+number%10) FROM numbers(25000);

-- Default limits are high, everything goes in one block.
SELECT max(blockSize())+sum(ignore(short, long2)) FROM file('03164_adapting_parquet_reader_output_size.parquet');
-- Small column doesn't take a lot of bytes, everything goes in one block.
SELECT max(blockSize())+sum(ignore(short)) FROM file('03164_adapting_parquet_reader_output_size.parquet') settings input_format_parquet_prefer_block_bytes=100000;
-- Specific number of rows requested.
SELECT max(blockSize())+sum(ignore(short, long2)) FROM file('03164_adapting_parquet_reader_output_size.parquet') settings input_format_parquet_max_block_size=64;
-- Tiny byte limit, reader bumps block size to 128 rows instead of 1 row.
SELECT max(blockSize())+sum(ignore(short, long2)) FROM file('03164_adapting_parquet_reader_output_size.parquet') settings input_format_parquet_prefer_block_bytes=30;

-- Intermediate byte limit. The two parquet reader implementations estimate row byte sizes slightly
-- differently it slightly differently and don't match exactly, so we round the result.
SELECT roundToExp2(max(blockSize())+sum(ignore(short, long2))) FROM file('03164_adapting_parquet_reader_output_size.parquet') settings input_format_parquet_prefer_block_bytes=700000;
SELECT roundToExp2(max(blockSize())+sum(ignore(short, long1, long2))) FROM file('03164_adapting_parquet_reader_output_size.parquet') settings input_format_parquet_prefer_block_bytes=700000;

-- Only the new parquet reader uses correct length estimate for dictionary-encoded strings.
SELECT roundToExp2(max(blockSize())+sum(ignore(short, long_low_cardinality))) FROM file('03164_adapting_parquet_reader_output_size.parquet') settings input_format_parquet_prefer_block_bytes=700000, input_format_parquet_use_native_reader_v3=1;
