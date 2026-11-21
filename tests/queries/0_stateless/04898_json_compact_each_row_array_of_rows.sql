-- https://github.com/ClickHouse/ClickHouse/issues/80408
-- JSONCompactEachRow should accept a wrapping array around rows (JSON.stringify of an array of rows).

-- Unwrapped rows still work with the default auto mode.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[123, "Hello"]\n[456, "World"]');

-- Wrapped rows, default auto.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[[123, "Hello"], [456, "World"]]');

-- Pretty-printed wrapping array.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[\n  [123, "Hello"],\n  [456, "World"]\n]');

-- Setting 1 forces a wrapping array.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[[123, "Hello"], [456, "World"]]') SETTINGS input_format_json_array_of_rows = 1;

-- Setting 1 rejects unwrapped rows.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[123, "Hello"]') SETTINGS input_format_json_array_of_rows = 1; -- { serverError INCORRECT_DATA }

-- Setting 0 never unwraps, so a wrapping array is incorrect data.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[[123, "Hello"], [456, "World"]]') SETTINGS input_format_json_array_of_rows = 0; -- { serverError INCORRECT_DATA }

-- Setting 0 still accepts ordinary unwrapped rows.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[123, "Hello"]\n[456, "World"]') SETTINGS input_format_json_array_of_rows = 0;

-- First column is Array: unwrapped data must not be treated as a wrapping array.
SELECT * FROM format(JSONCompactEachRow, 'a Array(UInt64), b String', '[[123, 456], "Hello"]\n[[789], "World"]');

-- First column is Array: wrapped data is one nesting level deeper.
SELECT * FROM format(JSONCompactEachRow, 'a Array(UInt64), b String', '[[[123, 456], "Hello"], [[789], "World"]]');

-- Unnamed Tuple adds nesting the same way Array does.
SELECT * FROM format(JSONCompactEachRow, 'a Tuple(UInt64, UInt64), b String', '[[123, 456], "Hello"]');
SELECT * FROM format(JSONCompactEachRow, 'a Tuple(UInt64, UInt64), b String', '[[[123, 456], "Hello"], [[1, 2], "World"]]');

-- Named Tuple is a JSON object, so expected nesting is 0.
SELECT * FROM format(JSONCompactEachRow, 'a Tuple(x UInt64, y UInt64), b String', '[{"x":1,"y":2}, "Hello"]');
SELECT * FROM format(JSONCompactEachRow, 'a Tuple(x UInt64, y UInt64), b String', '[[{"x":1,"y":2}, "Hello"]]');

-- Nested Array and LowCardinality wrappers still count Array dimensions.
SELECT * FROM format(JSONCompactEachRow, 'a Array(Array(UInt64)), b String', '[[[1, 2]], "Hello"]');
SELECT * FROM format(JSONCompactEachRow, 'a Array(Array(UInt64)), b String', '[[[[1, 2]], "Hello"]]');
SELECT * FROM format(JSONCompactEachRow, 'a LowCardinality(Array(UInt64)), b String', '[[123], "Hello"]');
SELECT * FROM format(JSONCompactEachRow, 'a LowCardinality(Array(UInt64)), b String', '[[[123], "Hello"]]');
SELECT * FROM format(JSONCompactEachRow, 'a Nullable(Array(UInt64)), b String', '[[123], "Hello"]');
SELECT * FROM format(JSONCompactEachRow, 'a Nullable(Array(UInt64)), b String', '[[[123], "Hello"]]');

-- WithNames: the first row is names, so Auto can unwrap a wrapping array even without using the first data type.
SELECT * FROM format(JSONCompactEachRowWithNames, 'a UInt64, b String', '["a","b"]\n[123, "Hello"]');
SELECT * FROM format(JSONCompactEachRowWithNames, 'a UInt64, b String', '[["a","b"], [123, "Hello"], [456, "World"]]');
SELECT * FROM format(JSONCompactEachRowWithNamesAndTypes, 'a UInt64, b String', '[["a","b"], ["UInt64","String"], [123, "Hello"]]');

-- Empty wrapping array is zero rows when unwrap is forced.
SELECT count() FROM format(JSONCompactEachRow, 'a UInt64, b String', '[]') SETTINGS input_format_json_array_of_rows = 1;

-- A wrapping array that is missing the closing ']' is incorrect data.
SELECT * FROM format(JSONCompactEachRow, 'a UInt64, b String', '[[123, "Hello"], [456, "World"]') SETTINGS input_format_json_array_of_rows = 1; -- { serverError INCORRECT_DATA }

-- Same wrapping-array support for the Strings variant. Fields are JSON strings, so Auto
-- unwraps on a leading `[[` even when the first column is Array.
SELECT * FROM format(JSONCompactStringsEachRow, 'a UInt64, b String', '[["123", "Hello"], ["456", "World"]]');
SELECT * FROM format(JSONCompactStringsEachRow, 'a Array(UInt64), b String', '[["[123, 456]", "Hello"], ["[789]", "World"]]');

-- Schema inference without column types: Auto stays conservative, setting 1 unwraps.
SELECT * FROM format(JSONCompactEachRow, '[[123, "Hello"], [456, "World"]]') SETTINGS input_format_json_array_of_rows = 1;

-- Strings fields cannot start with '[', so Auto can unwrap during schema inference.
SELECT * FROM format(JSONCompactStringsEachRow, '[["123", "Hello"], ["456", "World"]]');

-- Keep a JSON array as a String field by disabling unwrap.
SELECT * FROM format(JSONCompactEachRow, 'a String, b String', '[["hello"], "world"]') SETTINGS input_format_json_array_of_rows = 0;

-- INSERT with a wrapping array.
DROP TABLE IF EXISTS t_json_compact_wrap;
CREATE TABLE t_json_compact_wrap (a UInt64, b String) ENGINE = Memory;
INSERT INTO t_json_compact_wrap FORMAT JSONCompactEachRow [[1, "x"], [2, "y"]];
SELECT * FROM t_json_compact_wrap ORDER BY a;
DROP TABLE t_json_compact_wrap;
