-- Tags: no-fasttest
-- Test: Verify custom parquet encoder rejects versions < 2.6 with clear error
-- Covers: ParquetBlockOutputFormat.cpp:86-87 error path for version < V2_6


-- Custom encoder (default) should reject version 1.0
SELECT 1 as x SETTINGS output_format_parquet_version='1.0', output_format_parquet_use_custom_encoder=1 FORMAT Parquet; -- { clientError NOT_IMPLEMENTED }

-- Custom encoder (default) should reject version 2.4
SELECT 1 as x SETTINGS output_format_parquet_version='2.4', output_format_parquet_use_custom_encoder=1 FORMAT Parquet; -- { clientError NOT_IMPLEMENTED }
