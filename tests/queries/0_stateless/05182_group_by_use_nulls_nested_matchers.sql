-- A matcher does not have to be the root of a projection item: `untuple` consumes its output as a
-- tuple. With `group_by_use_nulls`, where the projection is resolved after the other clauses, such a
-- nested matcher has to be expanded in advance as well, otherwise its `REPLACE` mappings do not reach
-- GROUP BY, HAVING, WHERE and ORDER BY, and the result differs from the same query without the setting.

SET enable_analyzer = 1;

SELECT '-- REPLACE inside untuple is visible in HAVING and ORDER BY';
SELECT untuple((* REPLACE (100 - c AS c),)) FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH ROLLUP HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT untuple((* REPLACE (100 - c AS c),)) FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH ROLLUP HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '-- The GROUP BY key of the ROLLUP total is Nullable, the other rows are the same';
SELECT untuple((* REPLACE (100 - c AS c),)), c FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH ROLLUP ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT untuple((* REPLACE (100 - c AS c),)), c FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH ROLLUP ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '-- REPLACE inside untuple is visible in WHERE';
SELECT untuple((* REPLACE (-c AS c),)) FROM (SELECT number AS c FROM numbers(4))
WHERE c > -2 GROUP BY GROUPING SETS ((), (c)) ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT untuple((* REPLACE (-c AS c),)) FROM (SELECT number AS c FROM numbers(4))
WHERE c > -2 GROUP BY GROUPING SETS ((), (c)) ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '-- A matcher nested two functions deep';
SELECT untuple(tuple(* REPLACE (-c AS c))) FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH CUBE ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT untuple(tuple(* REPLACE (-c AS c))) FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH CUBE ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '-- APPLY inside untuple';
SELECT untuple((* APPLY (x -> x + 10),)) FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH ROLLUP ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT untuple((* APPLY (x -> x + 10),)) FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH ROLLUP ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '-- count still drops the asterisk instead of expanding it';
SELECT count(*), countIf(*, c > 0) FROM (SELECT number AS c, number + 1 AS d FROM numbers(3))
GROUP BY c WITH ROLLUP ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT count(*), countIf(*, c > 0) FROM (SELECT number AS c, number + 1 AS d FROM numbers(3))
GROUP BY c WITH ROLLUP ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;
