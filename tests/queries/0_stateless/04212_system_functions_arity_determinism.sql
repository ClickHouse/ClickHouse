-- Tags: no-parallel
-- New columns on system.functions: deterministic, higher_order.

SELECT '-- columns exist with the expected types';
SELECT name, type FROM system.columns
WHERE database = 'system' AND table = 'functions'
    AND name IN ('deterministic', 'higher_order')
ORDER BY name;

SELECT '-- deterministic non-higher-order function';
SELECT name, deterministic, higher_order
FROM system.functions WHERE name = 'plus';

SELECT '-- non-deterministic function';
SELECT name, deterministic, higher_order
FROM system.functions WHERE name = 'today';

SELECT '-- another deterministic non-higher-order function';
SELECT name, deterministic, higher_order
FROM system.functions WHERE name = 'concat';

SELECT '-- non-deterministic function';
SELECT name, deterministic, higher_order
FROM system.functions WHERE name = 'rand';

SELECT '-- higher-order functions are flagged (array, map and arraySort variants)';
SELECT name, higher_order
FROM system.functions
WHERE name IN ('arrayMap', 'arrayFilter', 'arrayFold', 'arraySort', 'mapApply', 'mapFilter')
ORDER BY name;

SELECT '-- non-higher-order functions are not flagged';
SELECT name, higher_order
FROM system.functions
WHERE name IN ('plus', 'concat', 'length')
ORDER BY name;

SELECT '-- aliases inherit metadata from the canonical function';
-- pow is an alias for power; both should report identical determinism/lambda flags.
SELECT name, deterministic, higher_order
FROM system.functions WHERE name IN ('pow', 'power') ORDER BY name;

SELECT '-- case-insensitive function entry has metadata of the canonical function';
-- CHAR_LENGTH is the case-insensitive name registered for length.
SELECT name, case_insensitive, deterministic, higher_order
FROM system.functions WHERE name = 'CHAR_LENGTH';

SELECT '-- aggregate functions: determinism is NULL by design';
SELECT count() FROM system.functions
WHERE is_aggregate = 1 AND deterministic IS NOT NULL;

SELECT '-- SQL UDFs: metadata is NULL (parse syntax field for arity)';
CREATE OR REPLACE FUNCTION test_udf_04212 AS (x, y) -> x + y;
SELECT name, deterministic, higher_order
FROM system.functions WHERE name = 'test_udf_04212';
DROP FUNCTION IF EXISTS test_udf_04212;
