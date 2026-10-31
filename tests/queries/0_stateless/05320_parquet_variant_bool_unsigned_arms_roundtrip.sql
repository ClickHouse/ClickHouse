-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- A mixed `Variant` column is written to the residual `Parquet` `VARIANT` value. The `Bool` arm must be
-- written as the boolean primitive (not `INT16`, as `Bool` is a custom-named `UInt8`), and the unsigned
-- arms, which `VARIANT` stores as wider signed primitives, must be read back into the same arm.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;
SET allow_suspicious_variant_types = 1;

SELECT '-- Variant(Bool, UInt8)';
INSERT INTO FUNCTION file(currentDatabase() || '_05320_bool_uint8.parquet', Parquet)
SELECT * FROM
(
    SELECT CAST(true, 'Variant(Bool, UInt8)') AS v
    UNION ALL SELECT CAST(false, 'Variant(Bool, UInt8)')
    UNION ALL SELECT CAST(7::UInt8, 'Variant(Bool, UInt8)')
    UNION ALL SELECT CAST(255::UInt8, 'Variant(Bool, UInt8)')
    UNION ALL SELECT CAST(NULL, 'Variant(Bool, UInt8)')
);
SELECT v, variantType(v) FROM file(currentDatabase() || '_05320_bool_uint8.parquet', Parquet, 'v Variant(Bool, UInt8)') ORDER BY variantType(v), toString(v);

SELECT '-- Variant(Bool, String)';
INSERT INTO FUNCTION file(currentDatabase() || '_05320_bool_string.parquet', Parquet)
SELECT * FROM
(
    SELECT CAST(true, 'Variant(Bool, String)') AS v
    UNION ALL SELECT CAST('x', 'Variant(Bool, String)')
);
SELECT v, variantType(v) FROM file(currentDatabase() || '_05320_bool_string.parquet', Parquet, 'v Variant(Bool, String)') ORDER BY variantType(v), toString(v);

SELECT '-- Unsigned arms';
INSERT INTO FUNCTION file(currentDatabase() || '_05320_unsigned.parquet', Parquet)
SELECT * FROM
(
    SELECT CAST(200::UInt8, 'Variant(String, UInt8, UInt16, UInt32, UInt64)') AS v
    UNION ALL SELECT CAST(60000::UInt16, 'Variant(String, UInt8, UInt16, UInt32, UInt64)')
    UNION ALL SELECT CAST(4000000000::UInt32, 'Variant(String, UInt8, UInt16, UInt32, UInt64)')
    UNION ALL SELECT CAST(10000000000::UInt64, 'Variant(String, UInt8, UInt16, UInt32, UInt64)')
    UNION ALL SELECT CAST('x', 'Variant(String, UInt8, UInt16, UInt32, UInt64)')
);
SELECT v, variantType(v) FROM file(currentDatabase() || '_05320_unsigned.parquet', Parquet, 'v Variant(String, UInt8, UInt16, UInt32, UInt64)') ORDER BY variantType(v), toString(v);
