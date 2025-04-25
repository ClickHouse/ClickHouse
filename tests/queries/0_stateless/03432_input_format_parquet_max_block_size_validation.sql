-- Test that setting input_format_parquet_max_block_size to 0 is not allowed
SET input_format_parquet_max_block_size = 0; -- { serverError BAD_ARGUMENTS }

-- Test that negative values are not allowed
SET input_format_parquet_max_block_size = -1; -- { serverError CANNOT_CONVERT_TYPE }

-- Test that valid positive values are allowed
SET input_format_parquet_max_block_size = 8192;
SELECT name, value FROM system.settings WHERE name = 'input_format_parquet_max_block_size' FORMAT TSV;

-- Test that the setting works with queries
SELECT 1 AS x FORMAT Parquet SETTINGS input_format_parquet_max_block_size = 16384;