-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Nested shredded `typed_value` reads used to require every top-level row to share the same
-- `metadata` dictionary and threw otherwise. The reader now maps each nested element row back to
-- the top-level row that owns its dictionary, so rows with different key sets (and therefore
-- different `metadata` blobs) read back correctly.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

CREATE TABLE test_04692_nested_variant
(
    j JSON(max_dynamic_paths=0, arr Array(JSON(max_dynamic_paths=0)))
)
ENGINE = Memory;

-- Every row uses a different set of keys, so no two rows share a `metadata` dictionary, and the
-- array elements live at a deeper repetition level than the `metadata` column.
INSERT INTO test_04692_nested_variant VALUES ('{"arr":[{"a":1},{"b":2}],"top1":"x"}'), ('{"arr":[{"c":3}],"top2":"y"}'), ('{"arr":[],"top3":"z"}'), ('{"arr":[{"a":4},{"c":5},{"d":6}],"top1":"w"}');

INSERT INTO FUNCTION file(currentDatabase() || '04692_parquet_variant_nested_per_row_metadata.parquet', Parquet)
SELECT j FROM test_04692_nested_variant;

SELECT 'whole column';
SELECT j
FROM file(
    currentDatabase() || '04692_parquet_variant_nested_per_row_metadata.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=0, arr Array(JSON(max_dynamic_paths=0)))')
ORDER BY toString(j);

SELECT 'nested subcolumn';
SELECT j.arr
FROM file(
    currentDatabase() || '04692_parquet_variant_nested_per_row_metadata.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=0, arr Array(JSON(max_dynamic_paths=0)))')
ORDER BY toString(j.arr);

SELECT 'as string';
SELECT j
FROM file(
    currentDatabase() || '04692_parquet_variant_nested_per_row_metadata.parquet',
    Parquet,
    'j String')
ORDER BY j;

DROP TABLE test_04692_nested_variant;
