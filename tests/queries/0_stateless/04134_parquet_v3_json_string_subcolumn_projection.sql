-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET input_format_parquet_enable_json_parsing = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04134_parquet_v3_json_string_subcolumn_projection.parquet', Parquet, 'j String')
SELECT '{"a":1,"b":"x"}'
UNION ALL
SELECT '{"a":2,"b":"y"}';

SELECT j.a, j.b
FROM file(
    currentDatabase() || '04134_parquet_v3_json_string_subcolumn_projection.parquet',
    Parquet,
    'j JSON(a Int64, b String)')
ORDER BY j.a
SETTINGS input_format_parquet_use_native_reader_v3 = 0
FORMAT TSVRaw;

SELECT j.a, j.b
FROM file(
    currentDatabase() || '04134_parquet_v3_json_string_subcolumn_projection.parquet',
    Parquet,
    'j JSON(a Int64, b String)')
ORDER BY j.a
SETTINGS input_format_parquet_use_native_reader_v3 = 1
FORMAT TSVRaw;
