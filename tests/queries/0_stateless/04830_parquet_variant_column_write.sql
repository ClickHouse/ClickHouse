-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- A real `Variant(...)` column is written as a `Parquet` `VARIANT` (previously it was rejected
-- with `UNKNOWN_TYPE`): the top-level dispatch routes `TypeIndex::Variant` through the `VARIANT`
-- write path, like `Dynamic`. Reading the file back with schema inference yields `Dynamic`.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET enable_json_type = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

SELECT '-- Top-level Variant column round-trips (read back as Dynamic)';
INSERT INTO FUNCTION file(currentDatabase() || '04830_variant_column.parquet', Parquet)
SELECT CAST(42::Int64, 'Variant(Int64, String, Array(Int64))') AS v
UNION ALL SELECT CAST('hello', 'Variant(Int64, String, Array(Int64))')
UNION ALL SELECT CAST([1::Int64, 2, 3]::Array(Int64), 'Variant(Int64, String, Array(Int64))')
UNION ALL SELECT CAST(NULL, 'Variant(Int64, String, Array(Int64))')
UNION ALL SELECT CAST(7::Int64, 'Variant(Int64, String, Array(Int64))');
SELECT v, dynamicType(v) FROM file(currentDatabase() || '04830_variant_column.parquet', Parquet, 'v Dynamic') ORDER BY toString(v);

SELECT '-- Reading back with an explicit Variant type hint';
SELECT v, variantType(v) FROM file(currentDatabase() || '04830_variant_column.parquet', Parquet, 'v Variant(Int64, String, Array(Int64))') ORDER BY toString(v);

SELECT '-- Variant nested inside Array';
INSERT INTO FUNCTION file(currentDatabase() || '04830_variant_in_array.parquet', Parquet)
SELECT CAST([1::Int64, 'x'], 'Array(Variant(Int64, String))') AS arr;
SELECT arr FROM file(currentDatabase() || '04830_variant_in_array.parquet', Parquet, 'arr Array(Dynamic)');

SELECT '-- Single-type Variant column (homogeneous rows shred to a typed_value)';
INSERT INTO FUNCTION file(currentDatabase() || '04830_variant_single.parquet', Parquet)
SELECT CAST(number::Int64, 'Variant(Int64, String)') AS v FROM numbers(3);
SELECT v, dynamicType(v) FROM file(currentDatabase() || '04830_variant_single.parquet', Parquet, 'v Dynamic') ORDER BY toString(v);

SELECT '-- Variant with JSON inside';
INSERT INTO FUNCTION file(currentDatabase() || '04830_variant_json.parquet', Parquet)
SELECT * FROM
(
    SELECT CAST('{"a":1}'::JSON, 'Variant(JSON, Int64)') AS v
    UNION ALL SELECT CAST(5::Int64, 'Variant(JSON, Int64)')
)
SETTINGS output_format_parquet_json_as_variant = 1;
SELECT v, dynamicType(v) FROM file(currentDatabase() || '04830_variant_json.parquet', Parquet, 'v Dynamic') ORDER BY toString(v);
