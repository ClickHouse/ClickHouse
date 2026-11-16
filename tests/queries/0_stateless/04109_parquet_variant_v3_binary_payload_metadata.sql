-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET output_format_parquet_json_as_variant = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04109_parquet_variant_v3_binary_payload_metadata.parquet', Parquet)
SELECT CAST('{"a":"x","b":1}' AS JSON) AS json
UNION ALL
SELECT CAST('{"a":"y","b":2}' AS JSON);

SELECT
    tupleElement(col, 1) AS name,
    tupleElement(col, 2) AS path,
    tupleElement(col, 5) AS physical_type,
    tupleElement(col, 6) AS logical_type
FROM
(
    SELECT arrayJoin(columns) AS col
    FROM file(currentDatabase() || '04109_parquet_variant_v3_binary_payload_metadata.parquet', ParquetMetadata)
)
ORDER BY path
FORMAT TSVRaw;
