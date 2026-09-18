-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_analyzer = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET output_format_parquet_json_as_variant = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04124_parquet_variant_schema_edge_cases_empty.parquet', Parquet)
SELECT CAST('{"a":1}' AS JSON(max_dynamic_paths=0, a UInt64)) AS j
WHERE 0;

SELECT tupleElement(column, 'path')
FROM file(currentDatabase() || '04124_parquet_variant_schema_edge_cases_empty.parquet', ParquetMetadata)
ARRAY JOIN columns AS column
ORDER BY tupleElement(column, 'path')
FORMAT TSVRaw;

INSERT INTO FUNCTION file(currentDatabase() || '04124_parquet_variant_schema_edge_cases_wrappers.parquet', Parquet)
SELECT CAST('{"arr":[{"value":1,"typed_value":"x"},{"typed_value":"y"},{"value":2}]}' AS JSON) AS j;

SELECT j.arr[].value.:Int64, j.arr[].`typed_value`.:String
FROM file(
    currentDatabase() || '04124_parquet_variant_schema_edge_cases_wrappers.parquet',
    Parquet,
    'j JSON')
FORMAT TSVRaw;
