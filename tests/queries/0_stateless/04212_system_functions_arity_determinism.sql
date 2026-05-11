-- Tags: no-parallel
-- no-parallel: SQL UDFs are global state and race with concurrent test workers (matches 02101_sql_user_defined_functions_create_or_replace.sql).

-- New columns on system.functions: is_deterministic, higher_order_function.

SELECT '-- columns exist with the expected types';
SELECT name, type FROM system.columns
WHERE database = 'system' AND table = 'functions'
    AND name IN ('is_deterministic', 'higher_order_function')
ORDER BY name;

SELECT '-- deterministic non-higher-order function';
SELECT name, is_deterministic, higher_order_function
FROM system.functions WHERE name = 'plus';

SELECT '-- non-deterministic function';
SELECT name, is_deterministic, higher_order_function
FROM system.functions WHERE name = 'today';

SELECT '-- another deterministic non-higher-order function (variadic)';
SELECT name, is_deterministic, higher_order_function
FROM system.functions WHERE name = 'concat';

SELECT '-- non-deterministic variadic function';
SELECT name, is_deterministic, higher_order_function
FROM system.functions WHERE name = 'rand';

SELECT '-- higher-order functions are flagged (array, map and arraySort variants)';
SELECT name, higher_order_function
FROM system.functions
WHERE name IN ('arrayMap', 'arrayFilter', 'arrayFold', 'arraySort', 'mapApply', 'mapFilter')
ORDER BY name;

SELECT '-- non-higher-order functions are not flagged';
SELECT name, higher_order_function
FROM system.functions
WHERE name IN ('plus', 'concat', 'length')
ORDER BY name;

SELECT '-- aliases inherit metadata from the canonical function';
-- pow is an alias for power; both should report identical determinism/lambda flags.
SELECT name, is_deterministic, higher_order_function
FROM system.functions WHERE name IN ('pow', 'power') ORDER BY name;

SELECT '-- case-insensitive function entry has metadata of the canonical function';
-- CHAR_LENGTH is the case-insensitive name registered for length.
SELECT name, case_insensitive, is_deterministic, higher_order_function
FROM system.functions WHERE name = 'CHAR_LENGTH';

SELECT '-- aggregate functions: determinism is NULL by design';
SELECT count() FROM system.functions
WHERE is_aggregate = 1 AND is_deterministic IS NOT NULL;

SELECT '-- SQL UDFs: metadata is NULL (parse syntax field for arity)';
DROP FUNCTION IF EXISTS test_udf_04212;
CREATE FUNCTION test_udf_04212 AS (x, y) -> x + y;
SELECT name, is_deterministic, higher_order_function
FROM system.functions WHERE name = 'test_udf_04212';
DROP FUNCTION test_udf_04212;
SELECT '-- after DROP the UDF is gone (count should be 0)';
SELECT count() FROM system.functions WHERE name = 'test_udf_04212';
