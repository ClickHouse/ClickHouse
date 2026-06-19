-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET use_variant_as_common_type = 1;

CREATE TABLE src
(
    id UInt64,
    d Dynamic
)
ENGINE = Memory;

INSERT INTO src VALUES
    (1, CAST([toInt64(1)], 'Dynamic')),
    (2, CAST([toInt64(2)], 'Dynamic')),
    (3, CAST(toInt64(42), 'Dynamic'));

INSERT INTO FUNCTION file(currentDatabase() || '04131_parquet_variant_mixed_array_scalar_roundtrip.parquet', Parquet)
SELECT *
FROM src;

CREATE TABLE dst
(
    id UInt64,
    d Dynamic
)
ENGINE = Memory;

INSERT INTO dst
SELECT *
FROM file(currentDatabase() || '04131_parquet_variant_mixed_array_scalar_roundtrip.parquet', Parquet);

SELECT
    id,
    dynamicType(d),
    d
FROM dst
ORDER BY id;
