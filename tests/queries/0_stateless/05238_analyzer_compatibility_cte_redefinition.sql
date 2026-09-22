-- Compatibility setting `analyzer_compatibility_cte_redefinition`: a CTE name may be defined more than once
-- in one WITH clause, a later definition shadowing the earlier ones as the query analysis before v24.3 did.
-- Expected values come from that analysis (26.8 with enable_analyzer = 0), except where noted.

DROP TABLE IF EXISTS t_cte_redefinition;
DROP VIEW IF EXISTS v_cte_redefinition;

-- Default: a redefinition is rejected, referenced or not.
WITH d AS (SELECT 1 AS id), d AS (SELECT 2 AS id) SELECT * FROM d; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH d AS (SELECT 1 AS id), d AS (SELECT 2 AS id) SELECT 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SET analyzer_compatibility_cte_redefinition = 1;

SELECT '-- 1 unreferenced duplicates';
WITH qb AS (SELECT 1 AS Id, 10 AS Shop), qb AS (SELECT 2 AS Id, 20 AS Shop)
SELECT a.Id, a.Shop FROM (SELECT 1 AS Id, 10 AS Shop UNION ALL SELECT 2, 20) AS a ORDER BY Id;

SELECT '-- 2 redefinition reads the previous definition';
WITH
    qb AS (SELECT Id, Shop FROM (SELECT 1 AS Id, 10 AS Shop UNION ALL SELECT 2, 20 UNION ALL SELECT 1, 30) WHERE Id = 1),
    qb AS (SELECT sum(Shop) AS x FROM qb)
SELECT a.x FROM qb AS a;

SELECT '-- 3 the query body reads the last definition';
WITH d AS (SELECT 1 AS id), d AS (SELECT 2 AS id) SELECT * FROM d;

SELECT '-- 4 chain of three definitions';
WITH a AS (SELECT 1 AS x), a AS (SELECT x + 1 AS x FROM a), a AS (SELECT x * 10 AS x FROM a) SELECT x FROM a;

SELECT '-- 5 two references to the redefined name';
WITH d AS (SELECT 1 AS id), d AS (SELECT id + 1 AS id FROM d) SELECT l.id, r.id FROM d AS l CROSS JOIN d AS r;

SELECT '-- 6 redefinition in a nested scope shadows the outer CTE';
WITH a AS (SELECT 1 AS x) SELECT * FROM (WITH a AS (SELECT 2 AS x), a AS (SELECT x + 1 AS x FROM a) SELECT * FROM a);

SELECT '-- 7 IN with a redefined CTE';
WITH s AS (SELECT 1 AS x), s AS (SELECT x + 1 AS x FROM s) SELECT number FROM numbers(5) WHERE number IN s;

SELECT '-- 8 the first definition reads the table of the same name';
CREATE TABLE t_cte_redefinition (x UInt8) ENGINE = Memory;
INSERT INTO t_cte_redefinition VALUES (5);
WITH
    t_cte_redefinition AS (SELECT x + 1 AS x FROM t_cte_redefinition),
    t_cte_redefinition AS (SELECT x * 2 AS x FROM t_cte_redefinition)
SELECT x FROM t_cte_redefinition;

SELECT '-- 9 progressive redefinition';
WITH
    base AS (SELECT number AS id, number * 10 AS val FROM numbers(3)),
    joined AS (SELECT id, val, val * 2 AS doubled FROM base),
    joined AS (SELECT *, doubled + 100 AS shifted FROM joined),
    joined AS (SELECT *, shifted * 3 AS final_val FROM joined)
SELECT * FROM joined ORDER BY id;

SELECT '-- 10 UNION CTE redefined';
WITH u AS (SELECT 1 AS x UNION ALL SELECT 2), u AS (SELECT sum(x) AS x FROM u) SELECT x FROM u;

SELECT '-- 11 consumer declared between two definitions';
-- The analysis before v24.3 bound `b` to the first `a` (result 1, 2). Here a reference that is not
-- inside a definition of `a` binds to the last definition of `a`.
WITH a AS (SELECT 1 AS v), b AS (SELECT v FROM a), a AS (SELECT 2 AS v), c AS (SELECT v FROM a)
SELECT b.v, c.v FROM b CROSS JOIN c;

SELECT '-- 12 view';
CREATE VIEW v_cte_redefinition AS
WITH
    raw AS (SELECT 1 AS id, 80 AS pct UNION ALL SELECT 2, 30),
    enriched AS (SELECT id, pct FROM raw),
    enriched AS (SELECT *, if(pct > 50, 'high', 'low') AS tier FROM enriched)
SELECT * FROM enriched ORDER BY id;
SELECT * FROM v_cte_redefinition;
SELECT * FROM v_cte_redefinition SETTINGS analyzer_compatibility_cte_redefinition = 0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT '-- 13 MATERIALIZED and RECURSIVE CTEs cannot be redefined';
SET enable_materialized_cte = 1;
WITH m AS MATERIALIZED (SELECT 1 AS x), m AS (SELECT x + 1 AS x FROM m) SELECT * FROM m; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH m AS (SELECT 1 AS x), m AS MATERIALIZED (SELECT x + 1 AS x FROM m) SELECT * FROM m; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH m AS MATERIALIZED (SELECT 1 AS x), m AS MATERIALIZED (SELECT 2 AS x) SELECT 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SET enable_materialized_cte = 0;
WITH m AS MATERIALIZED (SELECT 1 AS x), m AS (SELECT 2 AS x) SELECT 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3), r AS (SELECT 10 AS n) SELECT * FROM r; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

DROP VIEW v_cte_redefinition;
DROP TABLE t_cte_redefinition;
