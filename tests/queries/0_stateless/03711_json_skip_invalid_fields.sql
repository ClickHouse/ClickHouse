-- Test type_json_skip_invalid_typed_paths setting

-- Test 1: JSON type column with typed paths - skip invalid field where type conversion fails
SELECT 'Test 1: Skip invalid typed path - string cannot be coerced to Int64';
SELECT '{"a": "not_an_int", "b": "valid", "c": 123}'::JSON(a Int64, b String, c Int32)
SETTINGS type_json_skip_invalid_typed_paths = 1;

-- Test 2: JSON type column - verify error is thrown when setting is disabled

SELECT 'Test 2: Verify error thrown when setting disabled';
SELECT '{"a": "not_an_int", "b": "valid", "c": 123}'::JSON(a Int64, b String, c Int32)
SETTINGS type_json_skip_invalid_typed_paths = 0; -- { serverError INCORRECT_DATA }
