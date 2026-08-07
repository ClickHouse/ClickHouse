-- Ensure that SELECT (case-insensitive) as a bareword identifier is not allowed in WITH CTE definitions.

-- These should fail: "select" in any case as a CTE name
WITH select AS (SELECT 1) SELECT * FROM select; -- { clientError SYNTAX_ERROR }
WITH SELECT AS (SELECT 1) SELECT * FROM SELECT; -- { clientError SYNTAX_ERROR }
WITH Select AS (SELECT 1) SELECT * FROM Select; -- { clientError SYNTAX_ERROR }
WITH sElEcT AS (SELECT 1) SELECT * FROM sElEcT; -- { clientError SYNTAX_ERROR }

-- Quoted identifiers should still work
WITH `select` AS (SELECT 1) SELECT * FROM `select`;
WITH "select" AS (SELECT 1) SELECT * FROM "select";

-- Identifiers containing "select" as a prefix, suffix, or substring should still work
WITH selecta AS (SELECT 1) SELECT * FROM selecta;
WITH select_something AS (SELECT 1) SELECT * FROM select_something;
WITH Selected AS (SELECT 1) SELECT * FROM Selected;
WITH preselect AS (SELECT 1) SELECT * FROM preselect;
WITH myselect AS (SELECT 1) SELECT * FROM myselect;
WITH reselected AS (SELECT 1) SELECT * FROM reselected;

-- "select" as an alias for an expression (not a CTE name) should still work
WITH 1 AS select SELECT select;

-- "select" as a bareword CTE name in a multi-CTE WITH should fail
WITH foo AS (SELECT 1), select AS (SELECT 2) SELECT * FROM foo; -- { clientError SYNTAX_ERROR }

-- Other identifiers should still work
WITH foo AS (SELECT 1) SELECT * FROM foo;
WITH x AS (SELECT 42 AS val) SELECT val FROM x;
