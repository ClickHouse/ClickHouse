-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Reading a `JSON` object whose value is an array of two or more scalars used to abort with an
-- out-of-bounds access in the `Parquet` `VARIANT` reader (nested array elements reuse the parent
-- row's shared `metadata` dictionary, which was indexed past its size). Regression test.

SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04403_parquet_variant_scalar_array_read.parquet', Parquet)
SELECT CAST('{"ints":[1,2,3],"strs":["a","b"],"one":[7],"empty":[]}' AS JSON) AS j;

-- whole value as JSON
SELECT j FROM file(currentDatabase() || '04403_parquet_variant_scalar_array_read.parquet', Parquet, 'j JSON')
FORMAT TSVRaw;

-- array subcolumn projections
SELECT j.ints, j.strs, j.one, j.empty
FROM file(currentDatabase() || '04403_parquet_variant_scalar_array_read.parquet', Parquet, 'j JSON')
FORMAT TSVRaw;
