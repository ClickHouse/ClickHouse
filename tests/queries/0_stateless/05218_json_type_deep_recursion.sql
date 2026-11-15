-- Tags: no-fasttest
-- no-fasttest: needs the RapidJSON parser, which the Fast test build does not include.

-- Deeply nested JSON documents must be rejected instead of exhausting the thread stack. The
-- RapidJSON parser (allow_simdjson = 0) builds the document iteratively, so it survives any depth
-- and the depth reaches the consumers that walk the parsed document.
SET allow_simdjson = 0;

-- Each case must have a single possible rejector: which of the limit and the checkStackSize
-- backstop comes first depends on the build, since frame sizes and the share of the stack the
-- backstop allows differ.

-- Raising the limit above any reachable depth leaves the backstop as the only rejector. Nested
-- objects go through the object traversal, nested arrays through the type inference of a dynamic
-- path value.
SELECT CAST(concat(repeat('{"x":', 300000), '0', repeat('}', 300000)) AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100000000; -- { serverError TOO_DEEP_RECURSION }
SELECT CAST(concat('{"a":', repeat('[', 300000), repeat(']', 300000), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100000000; -- { serverError TOO_DEEP_RECURSION }

-- Extraction is bounded by the limit, and a value that exceeds it is a failure to extract, which
-- yields the default like any other. All the extractors of a value as text agree on that.
SELECT length(JSONExtract(concat(repeat('[', 300000), repeat(']', 300000)), 'String'));
SELECT length(JSONExtractRaw(concat(repeat('[', 300000), repeat(']', 300000))));
SELECT length(JSONExtractString(concat(repeat('[', 300000), repeat(']', 300000))));
SELECT JSONExtractArrayRaw(concat('[1,', repeat('[', 300000), repeat(']', 300000), ',2]'));
SELECT JSONExtractKeysAndValuesRaw(concat('{"a":1,"b":', repeat('[', 300000), repeat(']', 300000), '}'));

-- Raising the limit above any reachable depth leaves the backstop as the only rejector here too.
SELECT JSONExtractRaw(concat(repeat('[', 300000), repeat(']', 300000))) FORMAT Null SETTINGS input_format_json_max_depth = 100000000; -- { serverError TOO_DEEP_RECURSION }

-- The limit governs in both directions. These depths stay shallow: a document that survives parsing
-- is walked again by consumers whose recursion this setting does not bound.
SELECT CAST(concat(repeat('{"x":', 50), '0', repeat('}', 50)) AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 20; -- { serverError INCORRECT_DATA }
SELECT CAST(concat('{"a":', repeat('[', 50), repeat(']', 50), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 20; -- { serverError INCORRECT_DATA }
SELECT CAST(concat(repeat('{"x":', 50), '0', repeat('}', 50)) AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100;
SELECT CAST(concat('{"a":', repeat('[', 50), repeat(']', 50), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100;

-- A value written as text into a String column is bounded by the limit as well, and exceeding it is
-- reported the same way rather than escaping as an exception.
SELECT CAST(concat('{"a":', repeat('[', 50), repeat(']', 50), '}') AS JSON(a String)) FORMAT Null SETTINGS input_format_json_max_depth = 20; -- { serverError INCORRECT_DATA }
SELECT CAST(concat('{"a":', repeat('[', 50), repeat(']', 50), '}') AS JSON(a LowCardinality(String))) FORMAT Null SETTINGS input_format_json_max_depth = 20; -- { serverError INCORRECT_DATA }

-- An element quoted in an error message is rendered shallowly, so the message stays small.
SELECT CAST(concat('{"a":', repeat('[', 200), '1', repeat(']', 200), '}') AS JSON(a UInt32)) FORMAT Null; -- { serverError INCORRECT_DATA }

-- Exceeding the limit is a parse error, so a too-deep row can be skipped like any other malformed row.
SELECT count() FROM format(JSONEachRow, 'x JSON', concat('{"x":', repeat('{"a":', 50), '0', repeat('}', 50), '}\n{"x":{"a":42}}'))
SETTINGS input_format_json_max_depth = 20, input_format_allow_errors_num = 1;

-- Shallow documents must still be parsed normally.
SELECT CAST('{"a" : {"b" : [1, 2, 3]}, "c" : "42"}' AS JSON);
SELECT JSONExtractRaw('{"a" : {"b" : [1, 2, 3]}}', 'a');
