-- Random settings limits: optimize_multiif_to_if=(1, 1)

-- A three-argument `multiIf` `GROUP BY` key with `ROLLUP`, `CUBE` or `GROUPING SETS` and `group_by_use_nulls`: the clauses
-- that read the key after the aggregation return what they return with `optimize_multiif_to_if = 0`, i.e. `NULL` where
-- the key is rolled up, also when the `multiIf` reads other keys.

SET group_by_use_nulls = 1;

SELECT multiIf(number > 0, number, 0) FROM numbers(3) GROUP BY 1 WITH ROLLUP ORDER BY 1 SETTINGS enable_positional_arguments = 1;
SELECT 'cube', multiIf(number > 0, number, 0) AS k FROM numbers(3) GROUP BY k WITH CUBE ORDER BY ALL;
SELECT 'grouping sets', multiIf(number > 0, number, 0) AS k, count() FROM numbers(3) GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL;
SELECT 'group by all', multiIf(number > 0, number, 0) AS k, count() FROM numbers(3) GROUP BY ALL WITH ROLLUP ORDER BY ALL;
SELECT 'having', multiIf(number > 0, number, 0) AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP HAVING k IS NULL OR k > 0 ORDER BY ALL;
SELECT 'expression', toString(multiIf(number > 0, number, 0)) AS s FROM numbers(3) GROUP BY multiIf(number > 0, number, 0) WITH ROLLUP ORDER BY ALL;

-- The `multiIf` reads other keys.
SELECT 'other keys', number, multiIf(number > 0, number, 0) AS k, count() FROM numbers(3) GROUP BY ROLLUP(number, k) ORDER BY ALL;
SELECT 'other keys, cube', number, multiIf(number > 0, number, 0) AS k, count() FROM numbers(2) GROUP BY CUBE(number, k) ORDER BY ALL;
SELECT 'other keys, grouping sets', number, multiIf(number > 0, number, 0) AS k, count() FROM numbers(2) GROUP BY GROUPING SETS ((number, k), (number)) ORDER BY ALL;
SELECT 'other string keys', a, b, multiIf(a = 'x', a, b) AS k, count() FROM (SELECT 'x' AS a, 'w' AS b) GROUP BY ROLLUP(a, b, k) ORDER BY ALL;
SELECT 'key of a key', multiIf(number > 0, number, 0) AS k, toString(k) AS s, count() FROM numbers(3) GROUP BY ROLLUP(k, s) ORDER BY ALL;
SELECT 'remote', multiIf(number > 0, number, 0) AS k, count() FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY k WITH ROLLUP ORDER BY ALL;

-- `optimize_multiif_to_if` still applies: the key and the clauses that read it after the aggregation all become `if`.
SELECT 'explain', countIf(explain ILIKE '%function_name: multiIf,%'), countIf(explain ILIKE '%function_name: if,%') FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT multiIf(number > 0, number, 0) FROM numbers(3) GROUP BY 1 WITH ROLLUP ORDER BY 1 SETTINGS enable_positional_arguments = 1);
