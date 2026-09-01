SET json_type_escape_dots_in_keys = 1;

-- A key holding more than one dot, below a parent: every dot in the key is escaped, not just the
-- first, and the separator dot is left alone.
SELECT '{"x" : {"a.b.c" : 42}}'::JSON(max_dynamic_paths = 0) AS json, JSONSharedDataPaths(json);

-- Empty keys carry no dot, so escaping must leave the separator-only paths untouched.
SELECT '{"" : {"" : 42}}'::JSON(max_dynamic_paths = 0) AS json, JSONSharedDataPaths(json);

SET json_type_escape_dots_in_keys = 0;

-- Without escaping, a dotted key and a nested object collapse onto one shared-data path.
SELECT '{"a.b" : 1, "a" : {"b" : 2}}'::JSON(max_dynamic_paths = 0); -- { serverError INCORRECT_DATA }
SELECT '{"a.b" : 1, "a" : {"b" : 2}}'::JSON(max_dynamic_paths = 0) AS json, JSONSharedDataPaths(json)
    SETTINGS type_json_skip_duplicated_paths = 1;
