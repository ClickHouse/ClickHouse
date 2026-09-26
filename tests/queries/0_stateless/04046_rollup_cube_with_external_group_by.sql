-- Tags: no-parallel-replicas
-- Coverage gap for PR #99228 (Replace Block to Chunk in Aggregator).
-- Verifies that ROLLUP and CUBE produce correct results when external aggregation
-- is triggered (writeToTemporaryFileImpl + GroupByModifierTransform::merge paths).
-- The only prior test (04023) only checks for a memory-limit error, not correctness.

SET optimize_group_by_function_keys = 0;

-- ROLLUP without use_nulls: compare external vs non-external results
SELECT 'rollup_external';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH ROLLUP
ORDER BY key1, key2, s
SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT 'rollup_normal';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH ROLLUP
ORDER BY key1, key2, s;

-- CUBE without use_nulls: compare external vs non-external results
SELECT 'cube_external';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH CUBE
ORDER BY key1, key2, s
SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT 'cube_normal';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH CUBE
ORDER BY key1, key2, s;

-- ROLLUP with use_nulls: compare external vs non-external results
SELECT 'rollup_nulls_external';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH ROLLUP
ORDER BY key1, key2, s
SETTINGS group_by_use_nulls = 1, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT 'rollup_nulls_normal';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH ROLLUP
ORDER BY key1, key2, s
SETTINGS group_by_use_nulls = 1;

-- CUBE with use_nulls: compare external vs non-external results
SELECT 'cube_nulls_external';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH CUBE
ORDER BY key1, key2, s
SETTINGS group_by_use_nulls = 1, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT 'cube_nulls_normal';
SELECT sum(val) AS s, key1, key2
FROM (SELECT number % 5 AS key1, number % 3 AS key2, number AS val FROM numbers(10000))
GROUP BY key1, key2 WITH CUBE
ORDER BY key1, key2, s
SETTINGS group_by_use_nulls = 1;
