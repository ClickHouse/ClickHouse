-- https://github.com/ClickHouse/ClickHouse/issues/121472
-- With `input_format_null_as_default = 0`, a JSON `null` for a non-`Nullable` numeric column must raise, not become 0 / nan.

-- Default: null is replaced by the default value.
SELECT * FROM format(JSONEachRow, 'a Int64', '{"a": 1}\n{"a": null}');
SELECT * FROM format(JSONEachRow, 'a Float64', '{"a": 1}\n{"a": null}');

SELECT * FROM format(JSONEachRow, 'a Int64', '{"a": 1}\n{"a": null}') SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT * FROM format(JSONEachRow, 'a UInt8', '{"a": 1}\n{"a": null}') SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT * FROM format(JSONEachRow, 'a Float64', '{"a": 1}\n{"a": null}') SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT * FROM format(JSONEachRow, 'a Array(Int64)', '{"a": [1, null]}') SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT * FROM format(JSONCompactEachRow, 'a Int64', '[1]\n[null]') SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- Nullable columns still receive NULL.
SELECT * FROM format(JSONEachRow, 'a Nullable(Int64)', '{"a": 1}\n{"a": null}') SETTINGS input_format_null_as_default = 0;
SELECT * FROM format(JSONEachRow, 'a Array(Nullable(Float64))', '{"a": [1, null]}') SETTINGS input_format_null_as_default = 0;

-- The scenario from the issue: the null is past the schema inference sample, so a non-Nullable type is inferred.
SELECT toTypeName(a) FROM format(JSONEachRow, '{"a": 1}\n{"a": null}') LIMIT 1
SETTINGS schema_inference_make_columns_nullable = 'auto', input_format_max_rows_to_read_for_schema_inference = 1;
SELECT * FROM format(JSONEachRow, '{"a": 1}\n{"a": null}')
SETTINGS schema_inference_make_columns_nullable = 'auto', input_format_max_rows_to_read_for_schema_inference = 1, input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
