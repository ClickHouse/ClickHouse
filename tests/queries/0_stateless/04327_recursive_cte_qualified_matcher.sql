-- A qualified matcher (cte.*) inside the recursive term of a recursive CTE used to abort the server with a
-- LOGICAL_ERROR ("Table expression ... data must be initialized"). The matcher bound to the synthetic
-- self-reference node whose analysis data is never initialized, instead of the initialized FROM-clause node.
-- It must now expand the same columns a qualified column (x.a) or an unqualified matcher (*) would.

SET enable_analyzer = 1;

-- Exact fuzzer shape: x.* is the identity here, so the recursion never terminates and must report a regular
-- query error (TOO_DEEP_RECURSION), not abort the server. Wrapped in count() so no partial rows stream before
-- the error fires.
SELECT count() FROM (WITH RECURSIVE x AS (SELECT 1 AS a UNION ALL SELECT x.* FROM x WHERE a < 3) SELECT * FROM x)
SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- Self-reference x.* that terminates (the base row does not pass the recursive filter): returns the base row.
WITH RECURSIVE x AS (SELECT 5 AS a UNION ALL SELECT x.* FROM x WHERE a < 3) SELECT * FROM x ORDER BY a;

-- Multiple columns via x.*.
WITH RECURSIVE x AS (SELECT 5 AS a, 6 AS b UNION ALL SELECT x.* FROM x WHERE a < 3) SELECT * FROM x ORDER BY a;

-- x.* propagates a multi-row seed through the recursive term.
WITH RECURSIVE x AS (SELECT number AS a FROM numbers(4) UNION ALL SELECT x.* FROM x WHERE a < 0) SELECT * FROM x ORDER BY a;

-- Cross-name shape: CTE named a, column named x.
WITH RECURSIVE a AS (SELECT 5 AS x UNION ALL SELECT a.* FROM a WHERE x < 3) SELECT * FROM a ORDER BY x;

-- A qualified matcher over a real derived table inside the recursive term still resolves normally.
WITH RECURSIVE x AS (SELECT 5 AS a UNION ALL SELECT sub.* FROM (SELECT a FROM x) AS sub WHERE a < 3) SELECT * FROM x ORDER BY a;

-- Plain (non-recursive) CTE qualified matcher must keep working.
WITH cte AS (SELECT 1 AS a, 2 AS b) SELECT cte.* FROM cte;

-- Recursive CTE referenced from the outer query with a qualified matcher.
WITH RECURSIVE t AS (SELECT 1 AS n, 1 AS f UNION ALL SELECT n + 1, f * (n + 1) FROM t WHERE n < 5) SELECT t.* FROM t ORDER BY n;

-- Aliased self-reference inside the recursive term resolves to the join-tree node and works.
WITH RECURSIVE x AS (SELECT 1 AS a UNION ALL SELECT t.a + 1 FROM x AS t WHERE a < 3) SELECT * FROM x ORDER BY a;
