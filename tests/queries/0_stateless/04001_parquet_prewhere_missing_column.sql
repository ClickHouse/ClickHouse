-- Tags: no-fasttest

-- Regression test: PREWHERE on a Parquet file with a column declared in the schema
-- but not present in the actual file. When the missing column appears both in the
-- PREWHERE condition and in the SELECT list, it stays in the format header as a
-- pass-through INPUT node in the prewhere DAG. The `add_prewhere_outputs` lambda
-- incorrectly added such INPUT nodes to `external_columns`, which prevented the
-- SchemaConverter from creating a missing-column entry. This caused "KeyCondition
-- uses PREWHERE output" or "PREWHERE appears to use its own output as input"
-- exceptions (LOGICAL_ERROR).

SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_allow_missing_columns = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04001.parquet')
    SELECT number FROM numbers(10);

-- `extra` is not in the Parquet file; it defaults to 0 with allow_missing_columns.
-- The SELECT uses both `number` and `extra`, so `extra` remains in format_header as
-- a pass-through. Without the fix, `add_prewhere_outputs` incorrectly added `extra`
-- to `external_columns`, preventing SchemaConverter from creating a missing column.
SELECT number, extra FROM file(currentDatabase() || '_04001.parquet', Parquet, 'number UInt64, extra UInt64')
    PREWHERE extra = 0
    ORDER BY number;
