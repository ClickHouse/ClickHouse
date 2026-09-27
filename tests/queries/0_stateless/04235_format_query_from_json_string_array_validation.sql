-- Regression: `readStringArray` must throw when a key exists but its value is
-- not a JSON array. Previously it silently returned an empty vector, which
-- converted malformed input (e.g. `name_parts` as a string) into a different
-- valid AST instead of failing.

-- `name_parts` provided as a string instead of an array — must be rejected.
SELECT formatQueryFromJSON('{"type":"Identifier","name_parts":"not_an_array"}'); -- { serverError BAD_ARGUMENTS }

-- `name_parts` provided as a number — also rejected.
SELECT formatQueryFromJSON('{"type":"Identifier","name_parts":42}'); -- { serverError BAD_ARGUMENTS }

-- `name_parts` as a proper array still works.
SELECT formatQueryFromJSON('{"type":"Identifier","name_parts":["a","b"]}');

-- Field absence is still tolerated.
SELECT formatQueryFromJSON('{"type":"Identifier","name":"x"}');
