-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_analyzer = 1; -- Subquery access to `Parquet` `VARIANT` subcolumns requires the analyzer.
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04119_parquet_variant_reserved_wrapper_names.parquet', Parquet)
SELECT CAST(raw AS JSON) AS j
FROM values(
    'raw String',
    ('{"value":1,"typed_value":{"value":2,"typed_value":"x"}}'),
    ('{"value":3,"typed_value":{"value":4,"typed_value":"y"}}'));

SELECT j.value.:Int64, j.`typed_value`.value.:Int64, j.`typed_value`.`typed_value`.:String
FROM file(
    currentDatabase() || '04119_parquet_variant_reserved_wrapper_names.parquet',
    Parquet,
    'j JSON')
ORDER BY j.value.:Int64
FORMAT TSVRaw;

SELECT count()
FROM
(
    SELECT j
    FROM file(
        currentDatabase() || '04119_parquet_variant_reserved_wrapper_names.parquet',
        Parquet,
        'j JSON')
)
WHERE j.`typed_value`.value.:Int64 = 4 AND j.`typed_value`.`typed_value`.:String = 'y'
FORMAT TSVRaw;
