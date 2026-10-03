-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.
-- Regression test for exception in the native `Parquet` `VARIANT` reader when a
-- filter on one `JSON` subcolumn is combined with reading another subcolumn from
-- tiny data pages.

SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET output_format_parquet_data_page_size = 128;
SET max_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04133_parquet_variant_filtered_projection_regression.parquet', Parquet)
SELECT CAST(concat(
    '{"kind":"', if(number % 2 = 0, 'commit', 'identity'),
    '","did":"did:', toString(number),
    '","time_us":', toString(number),
    ',"commit":{"collection":"c', toString(number % 7),
    '","operation":"', if(number % 3 = 0, 'create', 'delete'), '"}}') AS JSON(
        max_dynamic_paths=0,
        kind LowCardinality(String),
        `commit.operation` LowCardinality(String),
        `commit.collection` LowCardinality(String),
        did String,
        time_us UInt64)) AS data
FROM numbers(100);

SELECT count()
FROM file(
    currentDatabase() || '04133_parquet_variant_filtered_projection_regression.parquet',
    Parquet,
    'data JSON(max_dynamic_paths=0, kind LowCardinality(String), `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String), did String, time_us UInt64)')
PREWHERE data.kind = 'commit'
FORMAT TSVRaw;

SELECT data.`commit.collection`
FROM file(
    currentDatabase() || '04133_parquet_variant_filtered_projection_regression.parquet',
    Parquet,
    'data JSON(max_dynamic_paths=0, kind LowCardinality(String), `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String), did String, time_us UInt64)')
PREWHERE data.kind = 'commit'
ORDER BY data.time_us
LIMIT 3
FORMAT TSVRaw;
