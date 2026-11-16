-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04128_parquet_variant_dynamic_temporal_types.parquet', Parquet)
SELECT CAST(toDate32('2024-01-02') AS Dynamic) AS d
UNION ALL
SELECT CAST(toDateTime('2024-01-02 03:04:05', 'UTC') AS Dynamic)
UNION ALL
SELECT CAST(toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC') AS Dynamic)
UNION ALL
SELECT CAST(CAST('12:34:56.123456', 'Time64(6)') AS Dynamic);

SELECT toTypeName(d)
FROM file(currentDatabase() || '04128_parquet_variant_dynamic_temporal_types.parquet', Parquet)
LIMIT 1
FORMAT TSVRaw;

SELECT dynamicType(d), toString(d)
FROM file(currentDatabase() || '04128_parquet_variant_dynamic_temporal_types.parquet', Parquet)
ORDER BY dynamicType(d), toString(d)
FORMAT TSVRaw;
