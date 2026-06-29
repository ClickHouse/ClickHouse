-- Tags: no-fasttest
SET enable_json_type = 1;
SET output_format_native_write_json_as_string = 0;

-- String -> JSON: accurateCastOrNull returns NULL for incompatible data (already worked before the fix)
SELECT 'String -> JSON';
SELECT accurateCastOrNull('{"a": "hello"}', 'JSON(a UInt32)');

-- JSON -> JSON: accurateCastOrNull should return NULL, not throw INCORRECT_DATA
SELECT 'JSON -> JSON';
SELECT accurateCastOrNull('{"a": "hello"}'::JSON, 'JSON(a UInt32)');

-- Tuple -> JSON: accurateCastOrNull should return NULL, not throw INCORRECT_DATA
SELECT 'Tuple -> JSON';
SELECT accurateCastOrNull(tuple('hello')::Tuple(a String), 'JSON(a UInt32)');

-- Map -> JSON: accurateCastOrNull should return NULL, not throw INCORRECT_DATA
SELECT 'Map -> JSON';
SELECT accurateCastOrNull(map('a', 'hello')::Map(String, String), 'JSON(a UInt32)');

-- Verify that valid casts still work correctly through all paths
SELECT 'Valid String -> JSON';
SELECT accurateCastOrNull('{"a": 42}', 'JSON(a UInt32)');

SELECT 'Valid JSON -> JSON';
SELECT accurateCastOrNull('{"a": 42}'::JSON, 'JSON(a UInt32)');

SELECT 'Valid Tuple -> JSON';
SELECT accurateCastOrNull(tuple(42)::Tuple(a UInt32), 'JSON(a UInt32)');
