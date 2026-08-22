-- Tags: no-fasttest
-- Reason: needs RapidJSON, which is not enabled in the fast test build.

-- The JSON parser is iterative, but merging and serializing the documents recurse over the tree.

SELECT length(JSONMergePatch('{}', concat(repeat('{"a":', 100000), '1', repeat('}', 100000)))); -- { serverError TOO_DEEP_RECURSION }
SELECT length(JSONMergePatch(concat(repeat('{"a":', 100000), '1', repeat('}', 100000)), '{}')); -- { serverError TOO_DEEP_RECURSION }

SELECT JSONMergePatch('{"a":{"b":1}}', '{"a":{"c":2}}');
