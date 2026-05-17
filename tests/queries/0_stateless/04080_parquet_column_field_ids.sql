-- Tags: no-fasttest
-- Reason: Parquet read/write path is not built in the Fast test image.
-- Verify that `output_format_parquet_column_field_ids` (Map override) and
-- `output_format_parquet_auto_assign_field_ids` (Iceberg-style auto assign) write Parquet
-- field_ids and don't break data roundtrip.

SET engine_file_truncate_on_insert = 1;

-- Explicit per-column overrides.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_custom.parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS output_format_parquet_column_field_ids = {'a': '10', 'b': '20', 'c': '30'};

SELECT a, b, c FROM file(currentDatabase() || '_04080_field_ids_custom.parquet');

-- Auto-assign only (no overrides): every column gets a sequential field_id starting at 1.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_auto.parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS output_format_parquet_auto_assign_field_ids = 1;

SELECT a, b, c FROM file(currentDatabase() || '_04080_field_ids_auto.parquet');

-- Auto-assign + partial override: overrides win; auto-assign fills remaining columns and
-- skips the ids already claimed.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_mixed.parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS output_format_parquet_auto_assign_field_ids = 1,
         output_format_parquet_column_field_ids = {'b': '1'};

SELECT a, b, c FROM file(currentDatabase() || '_04080_field_ids_mixed.parquet');

-- Default path (neither setting): writing still works, no field_ids emitted.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_none.parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c;

SELECT a, b, c FROM file(currentDatabase() || '_04080_field_ids_none.parquet');

-- Error: override references a column that isn't in the output.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_err.parquet')
SELECT 1 AS a
SETTINGS output_format_parquet_column_field_ids = {'missing': '1'}; -- { serverError BAD_ARGUMENTS }

-- Error: override doesn't cover every column when auto-assign is off.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_err.parquet')
SELECT 1 AS a, 2 AS b
SETTINGS output_format_parquet_column_field_ids = {'a': '1'}; -- { serverError BAD_ARGUMENTS }

-- Error: two overrides claim the same id.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_err.parquet')
SELECT 1 AS a, 2 AS b
SETTINGS output_format_parquet_column_field_ids = {'a': '1', 'b': '1'}; -- { serverError BAD_ARGUMENTS }

-- Error: value is not an integer.
INSERT INTO FUNCTION file(currentDatabase() || '_04080_field_ids_err.parquet')
SELECT 1 AS a
SETTINGS output_format_parquet_column_field_ids = {'a': 'oops'}; -- { serverError BAD_ARGUMENTS }
