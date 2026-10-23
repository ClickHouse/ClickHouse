-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04142_parquet_variant_metadata_count_width.parquet', Parquet)
WITH
    arrayConcat([''], arrayMap(i -> CAST(char(i), 'String'), range(1, 256))) AS keys,
    arrayMap(i -> toInt64(i), range(0, 256)) AS values
SELECT CAST(CAST((keys, values), 'Map(String, Int64)') AS Dynamic) AS d
UNION ALL
SELECT CAST(42 AS Dynamic);

SELECT count(), countIf(length(d) > 2)
FROM file(
    currentDatabase() || '04142_parquet_variant_metadata_count_width.parquet',
    Parquet,
    'd String')
FORMAT TSVRaw;
