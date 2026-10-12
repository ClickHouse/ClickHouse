-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_analyzer = 1; -- Subquery access to `Parquet` `VARIANT` subcolumns requires the analyzer.
SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04112_parquet_variant_direct_subcolumns.parquet', Parquet)
SELECT
    if(number = 0, CAST('str' AS Dynamic), CAST(CAST(number AS Int64) AS Dynamic)) AS d,
    CAST(if(number = 0, '{"a":42}', '{"a":"x"}') AS JSON) AS json
FROM numbers(2);

SELECT d.String, d.Int64
FROM file(
    currentDatabase() || '04112_parquet_variant_direct_subcolumns.parquet',
    Parquet,
    'd Dynamic')
ORDER BY ifNull(d.Int64, toInt64(0))
FORMAT TSVRaw;

SELECT count()
FROM
(
    SELECT d
    FROM file(
        currentDatabase() || '04112_parquet_variant_direct_subcolumns.parquet',
        Parquet,
        'd Dynamic')
)
WHERE d.String = 'str'
FORMAT TSVRaw;

SELECT dynamicType(json.a), json.a
FROM
(
    SELECT json
    FROM file(
        currentDatabase() || '04112_parquet_variant_direct_subcolumns.parquet',
        Parquet,
        'json JSON')
)
ORDER BY dynamicType(json.a), toString(json.a)
FORMAT TSVRaw;

SELECT count()
FROM
(
    SELECT json
    FROM file(
        currentDatabase() || '04112_parquet_variant_direct_subcolumns.parquet',
        Parquet,
        'json JSON')
)
WHERE dynamicType(json.a) = 'Int64' AND toString(json.a) = '42'
FORMAT TSVRaw;
