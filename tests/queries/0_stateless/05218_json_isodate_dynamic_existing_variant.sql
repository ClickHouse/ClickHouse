-- The MongoDB shell `ISODate("...")` wrapper is understood only where the target type is already
-- known to be `DateTime64`. `Dynamic` must keep rejecting it even after an earlier row has already
-- created a `DateTime64` variant (quoted date-time strings are inferred as `DateTime64` by default),
-- and at any nesting depth, e.g. inside an array once an `Array(DateTime64)` variant exists.

SET session_timezone = 'UTC';

SELECT d, dynamicType(d) FROM format(JSONEachRow, 'd Dynamic', '{"d": "2024-05-29T23:16:12.256Z"}');

SELECT d, dynamicType(d) FROM format(JSONEachRow, 'd Dynamic', '{"d": "2024-05-29T23:16:12.256Z"}\n{"d": ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError INCORRECT_DATA }
SELECT d, dynamicType(d) FROM format(JSONEachRow, 'd Dynamic', '{"d": "2024-05-29T23:16:12.256Z"}\n{"d": new ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT d, dynamicType(d) FROM format(JSONEachRow, 'd Dynamic', '{"d": ["2024-05-29T23:16:12.256Z"]}\n{"d": [ISODate("2024-05-29T23:16:12.256Z")]}'); -- { serverError INCORRECT_DATA }
SELECT d, dynamicType(d) FROM format(JSONEachRow, 'd Array(Dynamic)', '{"d": ["2024-05-29T23:16:12.256Z"]}\n{"d": [ISODate("2024-05-29T23:16:12.256Z")]}'); -- { serverError INCORRECT_DATA }

-- The strictness is local to `Dynamic`: a declared `DateTime64` arm of `Variant` still accepts the wrapper.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(DateTime64(3), String)', '{"v": "2024-05-29T23:16:12.256Z"}\n{"v": ISODate("2024-05-29T23:16:12.256Z")}') ORDER BY variantType(v), toString(v);
