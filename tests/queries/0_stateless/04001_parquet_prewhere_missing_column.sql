-- Tags: no-fasttest

-- Regression test: PREWHERE on Parquet file with a column declared in the schema
-- but not present in the actual file. This triggered "PREWHERE appears to use its
-- own output as input" because `add_prewhere_outputs` incorrectly added pass-through
-- INPUT columns to `external_columns`, preventing the SchemaConverter from creating
-- a missing column entry.

SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_allow_missing_columns = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04001.parquet')
    SELECT number FROM numbers(10);

-- `extra` is not in the Parquet file; with allow_missing_columns it defaults to 0.
-- PREWHERE uses `extra` as both input (to evaluate the condition) and output (pass-through).
SELECT number FROM file(currentDatabase() || '_04001.parquet', Parquet, 'number UInt64, extra UInt64')
    PREWHERE extra = 0
    ORDER BY number;
