-- With `group_by_use_nulls`, the matchers of the projection are expanded before GROUP BY, so that `REPLACE`
-- reaches the other clauses and positional arguments refer to the expanded columns. The expressions produced
-- by the `APPLY` and `REPLACE` transformers must nevertheless see the GROUP BY keys as Nullable, as the same
-- expressions written without a matcher do: `isNull` and `toTypeName` are folded into constants on resolution.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT '-- APPLY lambda';
SELECT * APPLY (x -> isNull(x)) FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;
SELECT isNull(k) FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- APPLY function';
SELECT * APPLY toTypeName FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- chained APPLY';
SELECT * APPLY (x -> tuple(x)) APPLY toTypeName FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- APPLY with a window function';
SELECT * APPLY (x -> (min(x) OVER (), any(toTypeName(x)) OVER ())) FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- REPLACE of a non-key column';
SELECT * REPLACE (toTypeName(k) AS v) FROM values('k String, v String', ('a', 'x'), ('b', 'y')) GROUP BY k, v WITH ROLLUP ORDER BY ALL;

SELECT '-- nested matcher';
SELECT untuple((* APPLY (x -> isNull(x)),)) FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- positional ORDER BY refers to the expressions resolved after GROUP BY';
SELECT * APPLY (x -> isNull(x)) FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY 1 DESC;

SELECT '-- REPLACE rewrites HAVING once';
SELECT * REPLACE (k || '!' AS k) FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP HAVING k = 'b!' ORDER BY ALL;

SELECT '-- matchers inside aggregate and grouping functions keep the original key type';
SELECT count(t.* REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY c WITH ROLLUP ORDER BY ALL;
SELECT * APPLY (x -> grouping(x)) FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- matchers of a named window referenced by the projection';
SELECT k, count() OVER w FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP WINDOW w AS (PARTITION BY * APPLY isNull) ORDER BY k NULLS LAST;
SELECT k, count() OVER w FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP WINDOW w AS (PARTITION BY isNull(k)) ORDER BY k NULLS LAST;

SELECT '-- REPLACE rewrites the WINDOW clause restored for the second expansion';
SELECT * REPLACE (-c AS c), groupArray(c) OVER w FROM (SELECT number AS c FROM numbers(3)) GROUP BY c WITH ROLLUP WINDOW w AS (ORDER BY c ASC NULLS LAST) ORDER BY ALL;
