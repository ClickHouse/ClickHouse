-- Deeply nested JSON documents must be rejected with TOO_DEEP_RECURSION instead of exhausting the
-- thread stack. The RapidJSON parser (allow_simdjson = 0) builds the document iteratively, so it
-- survives any depth and the depth reaches the consumers that walk the parsed document.
SET allow_simdjson = 0;

-- Nested objects: traversal of the parsed object when inserting into a JSON column.
SELECT CAST(concat(repeat('{"x":', 300000), '0', repeat('}', 300000)) AS JSON) FORMAT Null; -- { serverError TOO_DEEP_RECURSION }

-- Nested arrays: type inference for a value of a dynamic path.
SELECT CAST(concat('{"a":', repeat('[', 300000), repeat(']', 300000), '}') AS JSON) FORMAT Null; -- { serverError TOO_DEEP_RECURSION }

-- The same nesting with input_format_json_max_depth raised far above it, so the explicit limit does
-- not short-circuit and the checkStackSize backstop has to reject it (and does so in any build).
SELECT CAST(concat('{"a":', repeat('[', 300000), repeat(']', 300000), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 100000000; -- { serverError TOO_DEEP_RECURSION }

-- Writing a parsed document back as a string (JSONExtractRaw, JSONExtract to String, shared data,
-- error messages) is not bounded by input_format_json_max_depth, only by the backstop.
SELECT JSONExtractRaw(concat(repeat('[', 300000), repeat(']', 300000))) FORMAT Null; -- { serverError TOO_DEEP_RECURSION }
SELECT JSONExtract(concat(repeat('[', 300000), repeat(']', 300000)), 'String') FORMAT Null; -- { serverError TOO_DEEP_RECURSION }

-- Nesting above the default limit is accepted once the limit is raised to cover it. The depth stays
-- close to the default, because a raised limit also deepens the recursions over the inferred type.
SELECT CAST(concat(repeat('{"x":', 1100), '0', repeat('}', 1100)) AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 1200;
SELECT CAST(concat('{"a":', repeat('[', 1100), repeat(']', 1100), '}') AS JSON) FORMAT Null SETTINGS input_format_json_max_depth = 1200;

-- Shallow documents must still be parsed normally.
SELECT CAST('{"a" : {"b" : [1, 2, 3]}, "c" : "42"}' AS JSON);
SELECT JSONExtractRaw('{"a" : {"b" : [1, 2, 3]}}', 'a');
