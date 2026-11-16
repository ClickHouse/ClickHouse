-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: the shared metadata dictionary of a shredded `VARIANT` write must include object
-- keys nested inside typed-path values that spill into the residual `value` encoder. A path declared
-- as `Array(JSON)` holds object elements (e.g. `{"x":2}`) whose keys must be present in the
-- dictionary, otherwise the write fails with a missing dictionary entry.

SET enable_json_type = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04617_parquet_variant_nested_residual_keys_typed_path.parquet', Parquet)
SELECT CAST(raw AS JSON(a Int64, arr Array(JSON))) AS j
FROM values(
    'raw String',
    ('{"a":1,"arr":[{"x":2}]}'),
    ('{"a":2,"arr":[{"y":3},{"x":4,"z":5}]}'),
    ('{"a":3,"arr":[]}'));

SELECT tupleElement(column, 'path')
FROM file(currentDatabase() || '04617_parquet_variant_nested_residual_keys_typed_path.parquet', ParquetMetadata)
ARRAY JOIN columns AS column
ORDER BY tupleElement(column, 'path')
FORMAT TSVRaw;

SELECT toJSONString(j), toTypeName(j)
FROM file(currentDatabase() || '04617_parquet_variant_nested_residual_keys_typed_path.parquet', Parquet)
ORDER BY toJSONString(j)
FORMAT TSVRaw;
