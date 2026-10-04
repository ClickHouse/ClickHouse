-- The MongoDB shell `ISODate("...")` wrapper is understood only by a `DateTime64` target (or a `Variant`
-- with a `DateTime64` arm). The generic JSON field tokenizer stays strict: skipping an unknown field and
-- schema inference must keep rejecting it, like any other non-JSON token.

SET session_timezone = 'UTC';

-- Skipped unknown field: no `DateTime64` target is involved.
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": ISODate("2024-05-29T23:16:12.256Z")}') SETTINGS input_format_skip_unknown_fields = 1; -- { serverError INCORRECT_DATA }
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": new ISODate("2024-05-29T23:16:12.256Z")}') SETTINGS input_format_skip_unknown_fields = 1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": [ISODate("2024-05-29T23:16:12.256Z")]}') SETTINGS input_format_skip_unknown_fields = 1; -- { serverError INCORRECT_DATA }
SELECT x FROM format(JSONCompactEachRowWithNames, 'x UInt8', '["x", "y"]\n[1, ISODate("2024-05-29T23:16:12.256Z")]') SETTINGS input_format_skip_unknown_fields = 1; -- { serverError INCORRECT_DATA }

-- Positive control: an ordinary skipped field is still fine.
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": "2024-05-29T23:16:12.256Z"}') SETTINGS input_format_skip_unknown_fields = 1;

-- Schema inference that samples a wrapper row after a quoted date-time row rejects it instead of keeping `DateTime64`.
SELECT ts FROM format(JSONEachRow, '{"ts": "2024-05-29T23:16:12.256Z"}\n{"ts": ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"ts": "2024-05-29T23:16:12.256Z"}\n{"ts": new ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- The wrapper still works for a declared `Variant` with a `DateTime64` arm, also nested in an array.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Array(DateTime64(3)), String)', '{"v": [ISODate("2024-05-29T23:16:12.256Z")]}');
