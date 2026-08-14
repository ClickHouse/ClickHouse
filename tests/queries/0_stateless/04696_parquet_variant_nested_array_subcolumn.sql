-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Reading only a subcolumn of a shredded `VARIANT` whose typed path is an array of nested
-- `VARIANT` wrappers: the wrappers have no `metadata` child of their own, so the shared `metadata`
-- primitive is appended to the primitive list while the array's subtree is built and ends up inside
-- the array output's primitive range even though it does not repeat at that level.

SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04696.parquet', Parquet)
SELECT CAST(v AS JSON(max_dynamic_paths = 0, arr Array(JSON(max_dynamic_paths = 0, a UInt64)))) AS json
FROM values('v String',
    ('{"arr":[{"a":1}],"k1":"x"}'),
    ('{"arr":[{"a":2},{"a":3}],"k2":"y"}'),
    ('{"arr":[],"k3":"z"}'));

SELECT json.arr
FROM file(
    currentDatabase() || '_04696.parquet',
    Parquet,
    'json JSON(max_dynamic_paths = 0, arr Array(JSON(max_dynamic_paths = 0, a UInt64)))');

SELECT json.arr.a
FROM file(
    currentDatabase() || '_04696.parquet',
    Parquet,
    'json JSON(max_dynamic_paths = 0, arr Array(JSON(max_dynamic_paths = 0, a UInt64)))');

SELECT json
FROM file(
    currentDatabase() || '_04696.parquet',
    Parquet,
    'json JSON(max_dynamic_paths = 0, arr Array(JSON(max_dynamic_paths = 0, a UInt64)))');
