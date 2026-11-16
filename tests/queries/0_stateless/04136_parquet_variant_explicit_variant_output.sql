-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04136_parquet_variant_explicit_variant_output.parquet', Parquet)
SELECT CAST(toDate32('2024-01-02') AS Dynamic) AS d
UNION ALL
SELECT CAST('x' AS Dynamic)
UNION ALL
SELECT CAST([toInt64(1), toInt64(2)] AS Dynamic);

SELECT toTypeName(d)
FROM file(
    currentDatabase() || '04136_parquet_variant_explicit_variant_output.parquet',
    Parquet,
    'd Variant(Date32, String, Array(Int64))')
LIMIT 1
FORMAT TSVRaw;

SELECT variantType(d), toString(d)
FROM file(
    currentDatabase() || '04136_parquet_variant_explicit_variant_output.parquet',
    Parquet,
    'd Variant(Date32, String, Array(Int64))')
ORDER BY variantType(d), toString(d)
FORMAT TSVRaw;
