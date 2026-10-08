-- Tags: no-fasttest
-- no-fasttest: needs the RapidJSON parser, which the Fast test build does not include.

-- Deeply nested JSON documents must be rejected instead of exhausting the thread stack. The
-- RapidJSON parser (allow_simdjson = 0) builds the document iteratively, so it survives any depth
-- and the depth reaches the consumers that walk the parsed document.
SET allow_simdjson = 0;

-- Nested objects: traversal of the parsed object when inserting into a JSON column.
SELECT CAST(concat(repeat('{"x":', 300000), '0', repeat('}', 300000)) AS JSON) FORMAT Null; -- { serverError INCORRECT_DATA }

-- Nested arrays: type inference for a value of a dynamic path.
SELECT CAST(concat('{"a":', repeat('[', 300000), repeat(']', 300000), '}') AS JSON) FORMAT Null; -- { serverError INCORRECT_DATA }

-- The same nesting with input_format_json_max_depth raised far above it, so the explicit limit does
-- not short-circuit and the checkStackSize backstop has to reject it (and does so in any build).
SELECT CAST(concat('{"a":', repeat('[', 300000), repeat(']', 300000), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100000000; -- { serverError TOO_DEEP_RECURSION }

-- Writing a parsed document back as a string (JSONExtractRaw, JSONExtract to String, shared data,
-- error messages) is not bounded by input_format_json_max_depth, only by the backstop.
SELECT JSONExtractRaw(concat(repeat('[', 300000), repeat(']', 300000))) FORMAT Null; -- { serverError TOO_DEEP_RECURSION }
SELECT JSONExtract(concat(repeat('[', 300000), repeat(']', 300000)), 'String') FORMAT Null; -- { serverError TOO_DEEP_RECURSION }

-- The limit governs in both directions. The depths stay shallow because a document that survives
-- parsing is walked again by consumers whose recursion this setting does not bound.
SELECT CAST(concat(repeat('{"x":', 50), '0', repeat('}', 50)) AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 20; -- { serverError INCORRECT_DATA }
SELECT CAST(concat('{"a":', repeat('[', 50), repeat(']', 50), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 20; -- { serverError INCORRECT_DATA }
SELECT CAST(concat(repeat('{"x":', 50), '0', repeat('}', 50)) AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100;
SELECT CAST(concat('{"a":', repeat('[', 50), repeat(']', 50), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100;

-- A too-deep row is a parse error, so it can be skipped like any other malformed row.
SELECT count() FROM format(JSONEachRow, 'x JSON', concat('{"x":', repeat('{"a":', 2000), '0', repeat('}', 2000), '}\n{"x":{"a":42}}'))
SETTINGS input_format_allow_errors_num = 1;

-- Shallow documents must still be parsed normally.
SELECT CAST('{"a" : {"b" : [1, 2, 3]}, "c" : "42"}' AS JSON);
SELECT JSONExtractRaw('{"a" : {"b" : [1, 2, 3]}}', 'a');
