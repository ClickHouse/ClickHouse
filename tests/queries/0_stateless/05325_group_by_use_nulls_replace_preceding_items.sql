-- The `REPLACE` transformer of a projection matcher rewrites only the projection items following it,
-- the same with and without `group_by_use_nulls`.

SET enable_analyzer = 1;

SELECT 'preceding item';
SELECT max(c) AS m, max(* REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT max(c) AS m, max(* REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'preceding argument';
SELECT tuple(max(c), max(* REPLACE (100 - c AS c))) FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT tuple(max(c), max(* REPLACE (100 - c AS c))) FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'following item';
SELECT max(* REPLACE (100 - c AS c)), max(c) AS m FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT max(* REPLACE (100 - c AS c)), max(c) AS m FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;
