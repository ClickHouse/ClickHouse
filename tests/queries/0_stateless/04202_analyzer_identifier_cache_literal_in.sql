-- Enabling the identifier resolve cache for literal IN sets (`x IN (1, 2, 3)`) must not
-- change results, including when combined with the contexts the cache is otherwise kept
-- away from (subquery/table IN, group_by_use_nulls, join_use_nulls, rewrite_in_to_join,
-- transform_null_in). Each scenario is run with the cache disabled and then enabled; the
-- two output blocks must be identical.

SET enable_analyzer = 1;

-- 1. A reused alias used as the left-hand side of several literal IN lists.
SELECT h IN (0, 2, 4) AS a, h IN (1, 3, 5) AS b, h IN (6, 8, 10) AS c
FROM (SELECT number * 2 AS h FROM numbers(8))
ORDER BY h SETTINGS enable_identifier_resolve_cache = 0;
SELECT h IN (0, 2, 4) AS a, h IN (1, 3, 5) AS b, h IN (6, 8, 10) AS c
FROM (SELECT number * 2 AS h FROM numbers(8))
ORDER BY h SETTINGS enable_identifier_resolve_cache = 1;

-- 2. Literal IN alongside a subquery IN and a CTE (table) IN in the same scope. The CTE IN
-- has an identifier right-hand side at parse time that resolution rewrites into a subquery
-- in place; the literal IN must not be affected and the scope bookkeeping must stay balanced.
WITH cte AS (SELECT number FROM numbers(5))
SELECT number IN (1, 2, 3) AS lit, number IN (SELECT number FROM numbers(4)) AS sub, number IN cte AS tbl
FROM numbers(8) ORDER BY number SETTINGS enable_identifier_resolve_cache = 0;
WITH cte AS (SELECT number FROM numbers(5))
SELECT number IN (1, 2, 3) AS lit, number IN (SELECT number FROM numbers(4)) AS sub, number IN cte AS tbl
FROM numbers(8) ORDER BY number SETTINGS enable_identifier_resolve_cache = 1;

-- 3. Literal IN nested inside a subquery IN's right-hand side.
SELECT number FROM numbers(10)
WHERE number IN (SELECT x FROM (SELECT number AS x FROM numbers(10)) WHERE x IN (2, 4, 6, 8))
ORDER BY number SETTINGS enable_identifier_resolve_cache = 0;
SELECT number FROM numbers(10)
WHERE number IN (SELECT x FROM (SELECT number AS x FROM numbers(10)) WHERE x IN (2, 4, 6, 8))
ORDER BY number SETTINGS enable_identifier_resolve_cache = 1;

-- 4. Literal IN over a function GROUP BY key with group_by_use_nulls and CUBE (the key
-- becomes Nullable for super-aggregates): the IN must agree with non-cached resolution.
SELECT k, k IN (0, 2, 4) AS in_lit, count()
FROM (SELECT number % 5 AS k FROM numbers(20))
GROUP BY k WITH CUBE ORDER BY k, in_lit
SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 0;
SELECT k, k IN (0, 2, 4) AS in_lit, count()
FROM (SELECT number % 5 AS k FROM numbers(20))
GROUP BY k WITH CUBE ORDER BY k, in_lit
SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 1;

-- 5. Literal IN on a column that becomes Nullable through an OUTER JOIN with join_use_nulls.
SELECT l.number AS x, r.number AS y, r.number IN (1, 2, 3) AS in_lit
FROM numbers(5) AS l LEFT JOIN (SELECT number FROM numbers(3)) AS r ON l.number = r.number
ORDER BY x SETTINGS join_use_nulls = 1, enable_identifier_resolve_cache = 0;
SELECT l.number AS x, r.number AS y, r.number IN (1, 2, 3) AS in_lit
FROM numbers(5) AS l LEFT JOIN (SELECT number FROM numbers(3)) AS r ON l.number = r.number
ORDER BY x SETTINGS join_use_nulls = 1, enable_identifier_resolve_cache = 1;

-- 6. Literal IN with rewrite_in_to_join enabled (the rewrite targets subquery/table IN; a
-- literal set must keep producing the same result).
SELECT number, number IN (3, 5, 7) AS in_lit FROM numbers(10)
ORDER BY number SETTINGS rewrite_in_to_join = 1, enable_identifier_resolve_cache = 0;
SELECT number, number IN (3, 5, 7) AS in_lit FROM numbers(10)
ORDER BY number SETTINGS rewrite_in_to_join = 1, enable_identifier_resolve_cache = 1;

-- 7. transform_null_in with a literal IN set containing NULL.
SELECT x, x IN (1, 3, NULL) AS in_lit
FROM (SELECT if(number % 3 = 0, NULL, number) AS x FROM numbers(9))
ORDER BY x NULLS FIRST SETTINGS transform_null_in = 1, enable_identifier_resolve_cache = 0;
SELECT x, x IN (1, 3, NULL) AS in_lit
FROM (SELECT if(number % 3 = 0, NULL, number) AS x FROM numbers(9))
ORDER BY x NULLS FIRST SETTINGS transform_null_in = 1, enable_identifier_resolve_cache = 1;

-- 8. globalIn with a literal tuple, a single-element literal IN (constant right-hand side),
-- and an array-literal IN (the `array` right-hand side form).
SELECT number, number GLOBAL IN (2, 4, 6) AS g, number IN (3) AS one, number IN [1, 3, 5] AS arr
FROM numbers(8) ORDER BY number SETTINGS enable_identifier_resolve_cache = 0;
SELECT number, number GLOBAL IN (2, 4, 6) AS g, number IN (3) AS one, number IN [1, 3, 5] AS arr
FROM numbers(8) ORDER BY number SETTINGS enable_identifier_resolve_cache = 1;
