-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET engine_file_truncate_on_insert = 1;
SET schema_inference_make_columns_nullable = 0;

-- The shredded layout of a `VARIANT` column is decided over the whole file, not over the first block
-- of rows. Here every row of the first blocks holds an array in the path `a` and only the last block
-- holds a number, so the path must stay in the residual `value` instead of being shredded as an array
-- whose `typed_value` would then have to carry an empty-array filler for the last row.
INSERT INTO FUNCTION file(currentDatabase() || '05238_parquet_variant_array_path_mixed_blocks_not_shredded.parquet', Parquet)
SELECT CAST(if(number < 2, concat('{"a":[', toString(number + 1), ']}'), '{"a":42}') AS JSON(max_dynamic_paths=1)) AS j
FROM numbers(3)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;

SELECT tupleElement(column, 'path')
FROM file(currentDatabase() || '05238_parquet_variant_array_path_mixed_blocks_not_shredded.parquet', ParquetMetadata)
ARRAY JOIN columns AS column
ORDER BY tupleElement(column, 'path')
FORMAT TSVRaw;

SELECT toTypeName(j), j.a.:Int64, toJSONString(j)
FROM file(currentDatabase() || '05238_parquet_variant_array_path_mixed_blocks_not_shredded.parquet', Parquet)
ORDER BY toJSONString(j)
FORMAT TSVRaw;
