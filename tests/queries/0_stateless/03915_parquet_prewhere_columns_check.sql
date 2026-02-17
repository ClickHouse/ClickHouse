-- Tags: no-fasttest

set input_format_parquet_use_native_reader_v3 = 1;
set engine_file_truncate_on_insert = 1;

-- Create a Parquet file with a UInt8 column containing values > 1 (and some zeros).
insert into function file(currentDatabase() || '_03915.parquet')
    SELECT 1;

SELECT * FROM file(currentDatabase() || '_03915.parquet', Parquet, 'c0 Int') PREWHERE c0 = 1; -- { serverError BAD_ARGUMENTS }
