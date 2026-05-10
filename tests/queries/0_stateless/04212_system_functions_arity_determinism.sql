-- Tags: no-parallel
-- no-parallel: SQL UDFs are global state and race with concurrent test workers (matches 02101_sql_user_defined_functions_create_or_replace.sql).

-- New columns on system.functions: min_arguments, max_arguments, is_deterministic, accepts_lambda.

SELECT '-- columns exist with the expected types';
SELECT name, type FROM system.columns
WHERE database = 'system' AND table = 'functions'
    AND name IN ('min_arguments', 'max_arguments', 'is_deterministic', 'accepts_lambda')
ORDER BY name;

SELECT '-- non-variadic deterministic function: arity is fixed and equal min == max';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'plus';

SELECT '-- non-variadic deterministic single-argument function';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'length';

SELECT '-- non-variadic non-deterministic function with zero arguments';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'today';

SELECT '-- variadic deterministic function: min/max are NULL, is_deterministic is 1';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'concat';

SELECT '-- variadic non-deterministic function: all metadata still reported';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'rand';

SELECT '-- higher-order functions are flagged (array, map and arraySort variants)';
SELECT name, accepts_lambda
FROM system.functions
WHERE name IN ('arrayMap', 'arrayFilter', 'arrayFold', 'arraySort', 'mapApply', 'mapFilter')
ORDER BY name;

SELECT '-- non-higher-order functions are not flagged';
SELECT name, accepts_lambda
FROM system.functions
WHERE name IN ('plus', 'concat', 'length')
ORDER BY name;

SELECT '-- aliases inherit metadata from the canonical function';
-- pow is an alias for power; both should report identical arity/determinism/lambda flags.
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name IN ('pow', 'power') ORDER BY name;

SELECT '-- case-insensitive function entry has metadata of the canonical function';
-- CHAR_LENGTH is the case-insensitive name registered for length.
SELECT name, case_insensitive, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'CHAR_LENGTH';

SELECT '-- aggregate functions: arity and determinism are NULL by design';
SELECT count() FROM system.functions
WHERE is_aggregate = 1
    AND (min_arguments IS NOT NULL OR max_arguments IS NOT NULL OR is_deterministic IS NOT NULL);

SELECT '-- SQL UDFs: arity is countable from the lambda parameter list';
DROP FUNCTION IF EXISTS test_udf_04212;
CREATE FUNCTION test_udf_04212 AS (x, y) -> x + y;
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'test_udf_04212';
DROP FUNCTION test_udf_04212;
SELECT '-- after DROP the UDF is gone (count should be 0)';
SELECT count() FROM system.functions WHERE name = 'test_udf_04212';
