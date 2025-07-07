-- Tags: no-fasttest

-- Test that setting input_format_parquet_max_block_size to 0 is not allowed
SET input_format_parquet_max_block_size = 0; -- { serverError BAD_ARGUMENTS }

-- Test that negative values are not allowed
SET input_format_parquet_max_block_size = -1; -- { serverError CANNOT_CONVERT_TYPE }

-- Test that valid positive values are allowed
SET input_format_parquet_max_block_size = 1024;
SELECT 'a' INTO OUTFILE '/dev/null' TRUNCATE FORMAT Parquet SETTINGS input_format_parquet_max_block_size = 1024;
