-- Tags: no-fasttest, no-parallel

-- Create an ~80 MB parquet file with one row group and one column.
insert into function file('03147_parquet_memory_tracking.parquet') select number from numbers(10000000) settings output_format_parquet_compression_method='none', output_format_parquet_row_group_size=1000000000000, engine_file_truncate_on_insert=1;

-- Try to read it with 60 MB memory limit. Should fail because we read the 80 MB column all at once.
select sum(ignore(*)) from file('03147_parquet_memory_tracking.parquet') settings max_memory_usage=60000000; -- { serverError CANNOT_ALLOCATE_MEMORY }

-- Try to read it with 500 MB memory limit, just in case.
select sum(ignore(*)) from file('03147_parquet_memory_tracking.parquet') settings max_memory_usage=500000000;

-- Truncate the file to avoid leaving too much garbage behind.
insert into function file('03147_parquet_memory_tracking.parquet') select number from numbers(1) settings engine_file_truncate_on_insert=1;
