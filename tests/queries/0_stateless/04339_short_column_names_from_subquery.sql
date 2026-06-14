-- Hybrid short-name fallback for subquery / CTE projection columns whose canonical
-- name is an unaliased qualified identifier (e.g. `b.f1`). Closes the following
-- open issues:
--   - https://github.com/ClickHouse/ClickHouse/issues/87022
--   - https://github.com/ClickHouse/ClickHouse/issues/66133
--   - https://github.com/ClickHouse/ClickHouse/issues/94858
-- Related (already closed): https://github.com/ClickHouse/ClickHouse/issues/94558
--
-- Behavior is gated by `analyzer_enable_short_column_names_from_subquery`. The
-- canonical (dotted) name keeps working in every case; the short name is an
-- additional way to refer to the column from the outer scope.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t04339;
CREATE TABLE t04339 (a Int32) ENGINE = Memory;
INSERT INTO t04339 VALUES (1);

-- =================================================================
-- Default-off: every previously-failing shape still fails exactly as
-- it did before this PR, so users who don't opt in see no behavior
-- change of any kind.
-- =================================================================

SET analyzer_enable_short_column_names_from_subquery = 0;

SELECT '-- default-off: canonical (dotted) name still works';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT `b.f1`, `b.f2` FROM t3 WHERE `b.f1` = 1;

SELECT '-- default-off: short name still fails -- issue #87022';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT * FROM t3 WHERE f1 = 1; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- default-off: subquery shape from issue #66133 still fails';

WITH t AS (SELECT 1 AS a)
SELECT o.a FROM (SELECT w.a FROM t AS s INNER JOIN t AS w USING (a)) AS o; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- default-off: 3-way self-join CTE shape from issue #94858 still fails';

WITH
    v1 AS (SELECT a FROM t04339),
    v2 AS (SELECT v1.a FROM v1, v1 AS x, v1 AS y WHERE v1.a = x.a AND v1.a = y.a)
SELECT * FROM v2 WHERE a = 1; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- default-off: materialized CTE short name still fails';

SET enable_materialized_cte = 1;

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS MATERIALIZED (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT * FROM t3 WHERE f1 = 1; -- { serverError UNKNOWN_IDENTIFIER }

SET enable_materialized_cte = 0;

-- =================================================================
-- Setting on: every previously-failing shape resolves, while the
-- canonical (dotted) name continues to work (purely additive).
-- =================================================================

SET analyzer_enable_short_column_names_from_subquery = 1;

SELECT '-- on: issue #87022 reproducer works';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT * FROM t3 WHERE f1 = 1;

SELECT '-- on: outer reference via dotted name still works (additive)';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT `b.f1`, `b.f2` FROM t3 WHERE `b.f1` = 1;

SELECT '-- on: reference via outer alias + short name';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT t3.f1, t3.f2 FROM t3 WHERE t3.f1 = 1;

SELECT '-- on: short name in subquery (not just CTE) -- issue #66133';

WITH t AS (SELECT 1 AS a)
SELECT o.a FROM (SELECT w.a FROM t AS s INNER JOIN t AS w USING (a)) AS o;

SELECT '-- on: 3-way self-join inside CTE -- issue #94858';

WITH
    v1 AS (SELECT a FROM t04339),
    v2 AS (SELECT v1.a FROM v1, v1 AS x, v1 AS y WHERE v1.a = x.a AND v1.a = y.a)
SELECT * FROM v2 WHERE a = 1;

SELECT '-- on: ambiguous short name (collision among siblings) still errors';

-- `single_join_prefer_left_table` is on by default and would strip the prefix from
-- the left-side projection (`a.f1` → `f1`), defusing the ambiguity at canonical-name
-- level. Turn it off so both projections keep their dotted canonical names and
-- exercise the short-name ambiguity rule directly.
SET single_join_prefer_left_table = 0;

WITH t1 AS (SELECT 1 AS f1),
     t2 AS (SELECT 1 AS f1),
     t3 AS (SELECT a.f1, b.f1 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT f1 FROM t3; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- on: dotted form still works in the ambiguous case';

WITH t1 AS (SELECT 1 AS f1),
     t2 AS (SELECT 1 AS f1),
     t3 AS (SELECT a.f1, b.f1 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT `a.f1`, `b.f1` FROM t3;

SET single_join_prefer_left_table = 1;

SELECT '-- on: explicit canonical alias shadows the short name';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS (SELECT b.f1, 10 AS f1 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT f1 FROM t3; -- explicit `f1` (the constant 10) wins canonical resolution

SELECT '-- on: explicit dotted projection in subquery (related: #94558)';

SELECT FIELD
FROM (
    SELECT B.FIELD
    FROM (SELECT NULL AS FIELD) AS A
    CROSS JOIN (SELECT NULL AS FIELD) AS B
);

SELECT '-- on: materialized CTE — same SQL-standard story, even though MATERIALIZED rewrites the CTE reference into a TableNode';

SET enable_materialized_cte = 1;

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS MATERIALIZED (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT * FROM t3 WHERE f1 = 1;

SELECT '-- on: materialized CTE — outer alias + short name';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS MATERIALIZED (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT t3.f1, t3.f2 FROM t3 WHERE t3.f1 = 1;

SELECT '-- on: materialized CTE — canonical dotted name still works (additive)';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS MATERIALIZED (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT `b.f1`, `b.f2` FROM t3 WHERE `b.f1` = 1;

SET enable_materialized_cte = 0;

SELECT '-- on: DESCRIBE shows canonical names (not short names)';

DESCRIBE (
    WITH t3 AS (SELECT b.f1, b.f2 FROM (SELECT 1 AS f1, 2 AS f2) AS a JOIN (SELECT 1 AS f1, 5 AS f2) AS b ON a.f1 = b.f1)
    SELECT * FROM t3
);

SELECT '-- on: SELECT * exposes canonical names';

WITH t1 AS (SELECT 1 AS f1, 2 AS f2),
     t2 AS (SELECT 1 AS f1, 5 AS f2),
     t3 AS (SELECT b.f1, b.f2 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1)
SELECT * FROM t3 FORMAT TSVWithNames;

DROP TABLE t04339;
