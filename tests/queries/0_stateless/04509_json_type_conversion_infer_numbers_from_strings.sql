SET session_timezone = 'UTC';
SET json_use_optimized_type_conversion = 1;

DROP TABLE IF EXISTS test_infer_nums;

-- Test 1: With try_infer_numbers_from_strings enabled, string "123" should be
-- inferred as Int64 when stored in Dynamic, not as String.
SELECT 'Test 1: try_infer_numbers_from_strings with JSON insert';
SET input_format_json_try_infer_numbers_from_strings = 1;
CREATE TABLE test_infer_nums (json JSON) ENGINE = Memory;
INSERT INTO test_infer_nums FORMAT JSONAsObject {"a" : "123", "b" : "not_a_number", "c" : "45.67"};

SELECT json.a, dynamicType(json.a), json.b, dynamicType(json.b), json.c, dynamicType(json.c) FROM test_infer_nums;
DROP TABLE test_infer_nums;

-- Test 2: With try_infer_numbers_from_strings enabled, inline CAST removing typed String path
-- should infer numbers from string values ("123" becomes Int64).
SELECT 'Test 2: try_infer_numbers_from_strings with typed path removal';
SELECT '{"a":"123"}'::JSON(a String) as json;
SELECT ('{"a":"123"}'::JSON(a String)::JSON).a as a, dynamicType(('{"a":"123"}'::JSON(a String)::JSON).a) as t;

-- Test 3: Without try_infer_numbers_from_strings, string "123" should stay as String.
SELECT 'Test 3: without try_infer_numbers_from_strings (default)';
SET input_format_json_try_infer_numbers_from_strings = 0;
CREATE TABLE test_infer_nums (json JSON) ENGINE = Memory;
INSERT INTO test_infer_nums FORMAT JSONAsObject {"a" : "123", "b" : "not_a_number"};

SELECT json.a, dynamicType(json.a), json.b, dynamicType(json.b) FROM test_infer_nums;
DROP TABLE test_infer_nums;

-- Test 4: With try_infer_numbers_from_strings and type conversion (changed typed paths).
SELECT 'Test 4: try_infer_numbers_from_strings with max_dynamic_paths change';
SET input_format_json_try_infer_numbers_from_strings = 1;
CREATE TABLE test_infer_nums (json JSON(max_dynamic_paths=3)) ENGINE = Memory;
INSERT INTO test_infer_nums FORMAT JSONAsObject {"a" : "123", "b" : "hello", "c" : "45.67"};

SELECT json.a, dynamicType(json.a), json.b, dynamicType(json.b), json.c, dynamicType(json.c) FROM test_infer_nums;
DROP TABLE test_infer_nums;

-- Test 5: Inline CAST with try_infer_numbers_from_strings — typed String path to untyped JSON.
SELECT 'Test 5: inline CAST JSON(a String) -> JSON';
SELECT '{"a" : "42"}'::JSON(a String)::JSON as json;
SELECT ('{"a" : "42"}'::JSON(a String)::JSON).a as a, dynamicType(('{"a" : "42"}'::JSON(a String)::JSON).a) as t;

-- Test 6: Changed typed path String -> UInt32 with try_infer_numbers_from_strings.
SELECT 'Test 6: changed typed path String -> UInt32';
CREATE TABLE test_infer_nums (json JSON(a String)) ENGINE = Memory;
INSERT INTO test_infer_nums FORMAT JSONAsObject {"a" : "123"};

ALTER TABLE test_infer_nums MODIFY COLUMN json JSON(a UInt32);
SELECT json.a FROM test_infer_nums;
DROP TABLE test_infer_nums;

-- Test 7: Array(String) typed path removed via inline CAST.
-- All-numeric array ["123","456"] is inferred as Array(Int64).
-- Mixed array ["123","hello"] stays Array(String) because types can't be unified.
SELECT 'Test 7: Array(String) typed path removal';
SELECT '{"a":["123","456"]}'::JSON(a Array(String))::JSON as json;
SELECT '{"a":["123","hello","45.67"]}'::JSON(a Array(String))::JSON as json;

-- Test 8: Array(String) typed path changed to Array(UInt32).
SELECT 'Test 8: Array(String) changed to Array(UInt32)';
CREATE TABLE test_infer_nums (json JSON(a Array(String))) ENGINE = Memory;
INSERT INTO test_infer_nums FORMAT JSONAsObject {"a" : ["123", "456"]};

ALTER TABLE test_infer_nums MODIFY COLUMN json JSON(a Array(UInt32));
SELECT json.a FROM test_infer_nums;
DROP TABLE test_infer_nums;
SET input_format_json_try_infer_numbers_from_strings = 0;
