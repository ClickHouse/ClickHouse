-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_enable_json_parsing = 1;
SET engine_file_truncate_on_insert = 1;
SET schema_inference_make_columns_nullable = 0;

INSERT INTO FUNCTION file(currentDatabase() || '04338_parquet_variant_nested_type_hints.parquet', Parquet)
SELECT
    CAST(tuple(CAST(42, 'Dynamic')), 'Tuple(d Dynamic)') AS t,
    CAST([CAST(42, 'Dynamic'), CAST('x', 'Dynamic')], 'Array(Dynamic)') AS a,
    CAST(map('k', CAST(42, 'Dynamic')), 'Map(String, Dynamic)') AS m,
    CAST(tuple(CAST('{"a":1}' AS JSON(max_dynamic_paths=1))), 'Tuple(j JSON(max_dynamic_paths=1))') AS tj;

SELECT toTypeName(t.d), dynamicType(t.d), toString(t.d)
FROM file(currentDatabase() || '04338_parquet_variant_nested_type_hints.parquet', Parquet);

SELECT toTypeName(a), arrayMap(x -> dynamicType(x), a), arrayMap(x -> toString(x), a)
FROM file(currentDatabase() || '04338_parquet_variant_nested_type_hints.parquet', Parquet);

SELECT toTypeName(m), dynamicType(m['k']), toString(m['k'])
FROM file(currentDatabase() || '04338_parquet_variant_nested_type_hints.parquet', Parquet);

SELECT toTypeName(tj.j), tj.j.a.:Int64
FROM file(currentDatabase() || '04338_parquet_variant_nested_type_hints.parquet', Parquet);
