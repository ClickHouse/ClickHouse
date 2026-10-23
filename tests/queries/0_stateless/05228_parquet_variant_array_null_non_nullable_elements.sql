-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: a `Parquet` `VARIANT` array may mix `null` with elements whose common type cannot be
-- inside `Nullable` (nested arrays, maps, `Dynamic`). The reader used to wrap that element type in
-- `Nullable` and fail with `ILLEGAL_TYPE_OF_ARGUMENT`; such arrays have no exact ClickHouse type and are
-- materialized as `Array(Dynamic)` instead. Arrays whose element type can be `Nullable` keep their exact type.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05228_arrays.parquet', Parquet)
SELECT arrayJoin([
    CAST([CAST([1], 'Dynamic'), CAST(NULL, 'Dynamic')], 'Dynamic'),
    CAST([CAST(CAST([1, 'x'], 'Array(Dynamic)'), 'Dynamic'), CAST(NULL, 'Dynamic')], 'Dynamic'),
    CAST([CAST(map('k', 1), 'Dynamic'), CAST(NULL, 'Dynamic')], 'Dynamic'),
    CAST([CAST([1], 'Dynamic'), CAST([2, 3], 'Dynamic')], 'Dynamic'),
    CAST([CAST(CAST('{"k":1}', 'JSON'), 'Dynamic'), CAST(NULL, 'Dynamic')], 'Dynamic'),
    CAST([CAST(1, 'Dynamic'), CAST(NULL, 'Dynamic')], 'Dynamic')]) AS d;

SELECT '-- Read as Dynamic';
SELECT d, dynamicType(d) FROM file(currentDatabase() || '_05228_arrays.parquet', Parquet, 'd Dynamic');

SELECT '-- Arrays with nulls inside a JSON path';
INSERT INTO FUNCTION file(currentDatabase() || '_05228_json_path.parquet', Parquet)
SELECT CAST(CAST(tuple([CAST([1], 'Dynamic'), CAST(NULL, 'Dynamic')], [CAST(CAST([1, 'x'], 'Array(Dynamic)'), 'Dynamic'), CAST(NULL, 'Dynamic')]), 'Tuple(a Array(Dynamic), b Array(Dynamic))'), 'JSON(a Array(Dynamic), b Array(Dynamic))') AS j;
SELECT j, JSONAllPathsWithTypes(j) FROM file(currentDatabase() || '_05228_json_path.parquet', Parquet, 'j JSON');
SELECT j.a, j.b FROM file(currentDatabase() || '_05228_json_path.parquet', Parquet, 'j JSON');
SELECT j.a, j.b FROM file(currentDatabase() || '_05228_json_path.parquet', Parquet, 'j JSON(a Array(Dynamic), b Array(Dynamic))');

SELECT '-- A top-level array is not an object and cannot be read as JSON';
SELECT d FROM file(currentDatabase() || '_05228_arrays.parquet', Parquet, 'd JSON'); -- { serverError INCORRECT_DATA }
