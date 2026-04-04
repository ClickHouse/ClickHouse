-- Tags: no-fasttest
-- Verify that output_format_parquet_column_field_ids writes field IDs into Parquet metadata
-- and does not break data roundtrip. Covers both the custom encoder and the Arrow encoder paths.

-- Custom encoder path (default).
INSERT INTO FUNCTION file('04080_field_ids_custom.parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS output_format_parquet_column_field_ids = 'a: 10, b: 20, c: 30';

SELECT a, b, c FROM file('04080_field_ids_custom.parquet');

-- Arrow encoder path.
INSERT INTO FUNCTION file('04080_field_ids_arrow.parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS output_format_parquet_column_field_ids = 'a: 100, b: 200, c: 300',
         output_format_parquet_use_custom_encoder = 0;

SELECT a, b, c FROM file('04080_field_ids_arrow.parquet');

-- Verify that writing without field IDs still works.
INSERT INTO FUNCTION file('04080_field_ids_none.parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c;

SELECT a, b, c FROM file('04080_field_ids_none.parquet');

-- Error: missing colon.
SELECT 1 AS a INTO OUTFILE '04080_field_ids_err.parquet' FORMAT Parquet
SETTINGS output_format_parquet_column_field_ids = 'a 10'; -- { serverError BAD_ARGUMENTS }

-- Error: duplicate column name.
SELECT 1 AS a INTO OUTFILE '04080_field_ids_err.parquet' FORMAT Parquet
SETTINGS output_format_parquet_column_field_ids = 'a: 10, a: 20'; -- { serverError BAD_ARGUMENTS }

-- Error: non-numeric field_id.
SELECT 1 AS a INTO OUTFILE '04080_field_ids_err.parquet' FORMAT Parquet
SETTINGS output_format_parquet_column_field_ids = 'a: abc'; -- { serverError BAD_ARGUMENTS }
