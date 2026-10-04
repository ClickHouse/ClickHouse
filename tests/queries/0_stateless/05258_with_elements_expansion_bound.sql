-- Every element of a `WITH` list is copied into the subqueries of the elements after it, together with the
-- copies it carries itself, so the propagation doubles the query with each element. The same holds for a
-- chain of common table expressions each reading the previous one twice. The expansion must stop at
-- `max_expanded_ast_elements` instead of exhausting the memory. The chains are kept short, and the limit
-- low, so that a server without the bound still finishes them.

SET max_expanded_ast_elements = 10000;

EXPLAIN AST optimize = 1 WITH (SELECT 1) AS v0, (SELECT v0) AS v1, (SELECT v1) AS v2, (SELECT v2) AS v3, (SELECT v3) AS v4, (SELECT v4) AS v5, (SELECT v5) AS v6, (SELECT v6) AS v7, (SELECT v7) AS v8, (SELECT v8) AS v9, (SELECT v9) AS v10, (SELECT v10) AS v11, (SELECT v11) AS v12, (SELECT v12) AS v13, (SELECT v13) AS v14, (SELECT v14) AS v15 SELECT v15; -- { serverError TOO_BIG_AST }

CREATE VIEW v_expansion AS WITH c0 AS (SELECT 1 AS x), c1 AS (SELECT a.x FROM c0 AS a, c0 AS b), c2 AS (SELECT a.x FROM c1 AS a, c1 AS b), c3 AS (SELECT a.x FROM c2 AS a, c2 AS b), c4 AS (SELECT a.x FROM c3 AS a, c3 AS b), c5 AS (SELECT a.x FROM c4 AS a, c4 AS b), c6 AS (SELECT a.x FROM c5 AS a, c5 AS b), c7 AS (SELECT a.x FROM c6 AS a, c6 AS b), c8 AS (SELECT a.x FROM c7 AS a, c7 AS b), c9 AS (SELECT a.x FROM c8 AS a, c8 AS b), c10 AS (SELECT a.x FROM c9 AS a, c9 AS b), c11 AS (SELECT a.x FROM c10 AS a, c10 AS b), c12 AS (SELECT a.x FROM c11 AS a, c11 AS b), c13 AS (SELECT a.x FROM c12 AS a, c12 AS b) SELECT x FROM c13; -- { serverError TOO_BIG_AST }

-- A short chain still works.
CREATE VIEW v_expansion AS WITH c0 AS (SELECT 1 AS x), c1 AS (SELECT a.x FROM c0 AS a, c0 AS b), c2 AS (SELECT a.x FROM c1 AS a, c1 AS b) SELECT x FROM c2;
SELECT count() FROM v_expansion;
DROP VIEW v_expansion;

-- `enable_global_with_statement` copies the `WITH` list of the first branch of a `UNION` into every other branch.
SET max_expanded_ast_elements = 1000;
EXPLAIN AST WITH 0 AS a0, 1 AS a1, 2 AS a2, 3 AS a3, 4 AS a4, 5 AS a5, 6 AS a6, 7 AS a7, 8 AS a8, 9 AS a9, 10 AS a10, 11 AS a11, 12 AS a12, 13 AS a13, 14 AS a14, 15 AS a15, 16 AS a16, 17 AS a17, 18 AS a18, 19 AS a19 SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0 UNION ALL SELECT a0; -- { serverError TOO_BIG_AST }
SELECT count() FROM (WITH 0 AS a0, 1 AS a1, 2 AS a2, 3 AS a3, 4 AS a4, 5 AS a5, 6 AS a6, 7 AS a7, 8 AS a8, 9 AS a9, 10 AS a10, 11 AS a11, 12 AS a12, 13 AS a13, 14 AS a14, 15 AS a15, 16 AS a16, 17 AS a17, 18 AS a18, 19 AS a19 SELECT a0 UNION ALL SELECT a1);

-- The limit of the current query is used while collecting the dependencies of a materialized view.
SET max_expanded_ast_elements = 100;
CREATE MATERIALIZED VIEW mv_expansion ENGINE = Memory AS WITH c0 AS (SELECT dummy AS x FROM system.one), c1 AS (SELECT a.x FROM c0 AS a, c0 AS b), c2 AS (SELECT a.x FROM c1 AS a, c1 AS b), c3 AS (SELECT a.x FROM c2 AS a, c2 AS b) SELECT x FROM c3; -- { serverError TOO_BIG_AST }
