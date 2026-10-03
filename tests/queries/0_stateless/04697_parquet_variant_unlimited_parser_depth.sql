-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- `max_parser_depth = 0` means "unlimited" for the `Parquet` `VARIANT` read and write depth guards,
-- matching the SQL parser. The native stack is then the only remaining bound, so both guards fall
-- back to `checkStackSize`. A payload nested deeply enough to exhaust the stack cannot be built
-- from SQL (the `JSON` parser caps input nesting far below that), so this pins the observable half
-- of the contract: nesting deeper than a small explicit limit is rejected, and is accepted with 0.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

SET max_parser_depth = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04697.parquet', Parquet)
SELECT materialize('{"a":{"b":{"c":{"d":{"e":{"f":{"g":1}}}}}}}')::JSON(max_dynamic_paths = 0) AS json;

SELECT json FROM file(currentDatabase() || '_04697.parquet', Parquet, 'json JSON(max_dynamic_paths = 0)');
SELECT json FROM file(currentDatabase() || '_04697.parquet', Parquet, 'json String');

SELECT json
FROM file(currentDatabase() || '_04697.parquet', Parquet, 'json String')
SETTINGS max_parser_depth = 5
FORMAT Null; -- { serverError TOO_DEEP_RECURSION }
