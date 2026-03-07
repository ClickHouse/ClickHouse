-- JSONAllPathsWithValues: returns Map(String, Dynamic) with path -> value (experimental).
-- Without the setting the function is disabled. Error message: "JSONAllPathsWithValues is
-- experimental. Set allow_experimental_json_all_paths_with_values = 1 to use it."
-- (Only the error code is validated here; the message is not asserted.)

SELECT JSONAllPathsWithValues('{"a": 1}'::JSON); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_json_all_paths_with_values = 1;

-- Scalar
SELECT '{"a": 42}'::JSON AS json, JSONAllPathsWithValues(json);

-- String value
SELECT '{"a": 42, "b": "Hello"}'::JSON AS json, JSONAllPathsWithValues(json);

-- Nested
SELECT '{"a": {"b": 1}}'::JSON AS json, JSONAllPathsWithValues(json);

-- Array
SELECT '{"a": [1, 2, 3]}'::JSON AS json, JSONAllPathsWithValues(json);

-- Mixed types
SELECT '{"a": 42, "b": "x", "c": 3.14}'::JSON AS json, JSONAllPathsWithValues(json);

-- Compare with JSONAllPathsWithTypes (path -> type name vs path -> value)
SELECT '{"a": 42, "b": "Hi"}'::JSON AS json, JSONAllPathsWithTypes(json) AS types, JSONAllPathsWithValues(json) AS values;

-- Deeper nesting
SELECT '{"a": {"b": {"c": 42}}}'::JSON AS json, JSONAllPathsWithValues(json);

-- Boolean and null
SELECT '{"a": true, "b": false, "c": null}'::JSON AS json, JSONAllPathsWithValues(json);

-- Array of objects
SELECT '{"arr": [{"x": 1}, {"y": 2}]}'::JSON AS json, JSONAllPathsWithValues(json);

-- Nested array
SELECT '{"a": [[1, 2], [3, 4]]}'::JSON AS json, JSONAllPathsWithValues(json);

-- Object inside array
SELECT '{"a": [1, {"b": 2}, 3]}'::JSON AS json, JSONAllPathsWithValues(json);

-- Empty object and array
SELECT '{"empty_obj": {}, "empty_arr": []}'::JSON AS json, JSONAllPathsWithValues(json);

-- Mixed: numbers, strings, bool, null, nested, array
SELECT '{"n": 1, "s": "hi", "b": true, "nil": null, "nest": {"x": 0}, "arr": [10, 20]}'::JSON AS json, JSONAllPathsWithValues(json);
