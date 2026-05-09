-- New columns on system.functions: min_arguments, max_arguments, is_deterministic, accepts_lambda.

SELECT '-- columns exist with the expected types';
SELECT name, type FROM system.columns
WHERE database = 'system' AND table = 'functions'
    AND name IN ('min_arguments', 'max_arguments', 'is_deterministic', 'accepts_lambda')
ORDER BY name;

SELECT '-- non-variadic deterministic function: arity is fixed and equal min == max';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'plus';

SELECT '-- non-variadic non-deterministic function with zero arguments';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'today';

SELECT '-- variadic deterministic function: min/max are NULL, is_deterministic is 1';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'concat';

SELECT '-- variadic non-deterministic function: all metadata still reported';
SELECT name, min_arguments, max_arguments, is_deterministic, accepts_lambda
FROM system.functions WHERE name = 'rand';

SELECT '-- higher-order array functions are flagged';
SELECT name, accepts_lambda
FROM system.functions
WHERE name IN ('arrayMap', 'arrayFilter', 'arrayFold', 'mapApply', 'mapFilter')
ORDER BY name;

SELECT '-- non-higher-order functions are not flagged';
SELECT name, accepts_lambda
FROM system.functions
WHERE name IN ('plus', 'concat', 'length')
ORDER BY name;

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
