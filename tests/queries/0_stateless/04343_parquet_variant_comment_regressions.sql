-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET schema_inference_make_columns_nullable = 0;

INSERT INTO FUNCTION file(currentDatabase() || '04343_parquet_variant_comment_regressions.parquet', Parquet)
SELECT CAST(
    tuple(
        CAST('{"x":1}' AS JSON(max_dynamic_paths=0)),
        tuple(CAST('{"x":2}' AS JSON(max_dynamic_paths=1)))),
    'Tuple(`a.b` JSON(max_dynamic_paths=0), a Tuple(b JSON(max_dynamic_paths=1)))') AS t;

SELECT
    toTypeName(tupleElement(t, 'a.b')),
    toTypeName(tupleElement(tupleElement(t, 'a'), 'b'))
FROM file(currentDatabase() || '04343_parquet_variant_comment_regressions.parquet', Parquet)
FORMAT TSVRaw;

INSERT INTO FUNCTION file(currentDatabase() || '04343_parquet_variant_residual_conflict.parquet', Parquet)
SELECT CAST('{"a":1,"b":1,"b":{"c":2}}' AS JSON(max_dynamic_paths=0, a UInt64)); -- { serverError BAD_ARGUMENTS }
