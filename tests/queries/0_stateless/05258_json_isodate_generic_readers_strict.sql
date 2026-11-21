-- The MongoDB shell `ISODate("...")` wrapper is understood only by a `DateTime64` target (or a `Variant`
-- with a `DateTime64` arm). Schema inference stays strict and must keep rejecting it, like any other
-- non-JSON token. A skipped field may contain it: the value is discarded, and the format cannot tell
-- an unknown field from a declared `DateTime64` column that the query does not read.

SET session_timezone = 'UTC';

-- Skipped unknown field.
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": ISODate("2024-05-29T23:16:12.256Z")}') SETTINGS input_format_skip_unknown_fields = 1;
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": new ISODate("2024-05-29T23:16:12.256Z")}') SETTINGS input_format_skip_unknown_fields = 1;
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": [ISODate("2024-05-29T23:16:12.256Z")]}') SETTINGS input_format_skip_unknown_fields = 1;
SELECT x FROM format(JSONCompactEachRowWithNames, 'x UInt8', '["x", "y"]\n[1, ISODate("2024-05-29T23:16:12.256Z")]') SETTINGS input_format_skip_unknown_fields = 1;
-- A malformed wrapper is still rejected when skipped.
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": ISODate("2024-05-29T23:16:12.256Z"}') SETTINGS input_format_skip_unknown_fields = 1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": ISODate(1)}') SETTINGS input_format_skip_unknown_fields = 1; -- { serverError CANNOT_PARSE_QUOTED_STRING }

-- Positive control: an ordinary skipped field is still fine.
SELECT x FROM format(JSONEachRow, 'x UInt8', '{"x": 1, "y": "2024-05-29T23:16:12.256Z"}') SETTINGS input_format_skip_unknown_fields = 1;

-- Schema inference that samples a wrapper row after a quoted date-time row rejects it instead of keeping `DateTime64`.
SELECT ts FROM format(JSONEachRow, '{"ts": "2024-05-29T23:16:12.256Z"}\n{"ts": ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"ts": "2024-05-29T23:16:12.256Z"}\n{"ts": new ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- The wrapper still works for a declared `Variant` with a `DateTime64` arm, also nested in an array.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Array(DateTime64(3)), String)', '{"v": [ISODate("2024-05-29T23:16:12.256Z")]}');
