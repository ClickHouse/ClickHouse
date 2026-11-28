-- A `Variant` tokenizes the MongoDB shell `ISODate("...")` wrapper only for its `DateTime64` variants:
-- an object or an array containing the wrapper must not be taken by a `String` variant as a raw string.

SET allow_experimental_variant_type = 1;

-- Positive controls: the wrapper itself reaches the `DateTime64` variant.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(String, DateTime64(3, \'UTC\'))', '{"v": ISODate("2024-05-29T23:16:12.256Z")}');
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(String, DateTime64(3, \'UTC\'))', '{"v": new ISODate("2024-05-29T23:16:12.256Z")}');
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(String, Array(DateTime64(3, \'UTC\')))', '{"v": [ISODate("2024-05-29T23:16:12.256Z")]}');

-- Valid JSON with the wrapper text inside a string is still read as a raw string.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(String, DateTime64(3, \'UTC\'))', '{"v": {"x": "ISODate(1)"}}');
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(String, DateTime64(3, \'UTC\'))', '{"v": ["ISODate(1)"]}');

-- An object or an array containing the wrapper is rejected instead of being read as `String`.
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime64(3, \'UTC\'))', '{"v": {"x": ISODate("2024-05-29T23:16:12.256Z")}}'); -- { serverError INCORRECT_DATA }
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime64(3, \'UTC\'))', '{"v": [ISODate("2024-05-29T23:16:12.256Z")]}'); -- { serverError INCORRECT_DATA }
SELECT v FROM format(JSONEachRow, 'v Variant(Array(String), DateTime64(3, \'UTC\'))', '{"v": [{"x": ISODate("2024-05-29T23:16:12.256Z")}]}'); -- { serverError INCORRECT_DATA }

-- Without a `DateTime64` variant the wrapper is not tokenized at all.
SELECT v FROM format(JSONEachRow, 'v Variant(String, UInt64)', '{"v": ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError INCORRECT_DATA }
SELECT v FROM format(JSONEachRow, 'v Variant(String, UInt64)', '{"v": {"x": ISODate("2024-05-29T23:16:12.256Z")}}'); -- { serverError INCORRECT_DATA }

-- The bad row is skipped as an error, the good one is read.
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(String, DateTime64(3, \'UTC\'))', '{"v": {"x": ISODate("2024-05-29T23:16:12.256Z")}}\n{"v": "ok"}') SETTINGS input_format_allow_errors_num = 1;
