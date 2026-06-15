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

SELECT '-- on: existing Nested-prefix resolution wins over the new short-name fallback';

-- For a subquery with `f1.x Array(...)`, `f1.y Array(...)`, `b.f1 ...` columns, outer
-- `SELECT f1` already resolves (on master) as the Nested-prefix collapse of `f1.x`
-- and `f1.y`. The new short-name path would also offer `f1` (the rightmost component
-- of `b.f1`), but the setting's additive contract means existing successful resolutions
-- must keep their target. The Nested-prefix path is therefore consulted first and
-- short-name fallback is suppressed for `f1` here.
SELECT f1 FROM (SELECT [1] AS `f1.x`, ['a'] AS `f1.y`, 42 AS `b.f1`);

SELECT '-- on: existing Nested-prefix wins across a CROSS JOIN sibling that offers short-name';

-- Same additivity guarantee, this time across a join boundary. The left subquery offers
-- `f1` only via the short-name fallback (canonical `b.f1`); the right subquery offers
-- `f1` via the existing Nested-prefix collapse of `f1.x` / `f1.y`. The two-pass
-- resolution at the join-tree level disables short-name fallback in pass 1 so the
-- right side's Nested-prefix wins, regardless of `single_join_prefer_left_table`.
SELECT f1 FROM (SELECT 42 AS `b.f1`) AS l CROSS JOIN (SELECT [1] AS `f1.x`, ['a'] AS `f1.y`) AS r;

SELECT '-- on: JOIN USING does not match a short-name-resolved column';

-- Bot finding (https://github.com/ClickHouse/ClickHouse/pull/107449#discussion_r3410399567):
-- A short-name-resolved `ColumnNode` still reports its canonical dotted name through
-- `getColumnName`, so a `USING (f1)` list cannot reliably merge a left key whose
-- canonical name is `b.f1` with a right key whose canonical name is `f1`. We therefore
-- suppress the short-name fallback during USING resolution, restoring master-equivalent
-- failure semantics for this corner case (the user can still resolve via the dotted
-- name or an explicit alias).
WITH t1 AS (SELECT 1 AS f1), t2 AS (SELECT 1 AS f1)
SELECT *
FROM (SELECT b.f1 FROM t1 AS a JOIN t2 AS b ON a.f1 = b.f1) AS q
JOIN (SELECT 1 AS f1) AS r USING (f1); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- on: SELECT * from sibling with `b.f1` does not over-qualify the existing `f1` projection';

-- Bot finding: enabling the setting should not change `SELECT *` output column names
-- for queries that don't reference the short-name. Removing the predicate-level
-- short-name binding (which was driving qualifier decisions) keeps headers stable.
SELECT * FROM (SELECT 1 AS f1) AS a, (SELECT 2 AS `b.f1`) AS q FORMAT TSVWithNames;

SELECT '-- on: alias-qualified short-name lookup is not hijacked by sibling literal column';

-- Bot finding (https://github.com/ClickHouse/ClickHouse/pull/107449#discussion_r3411043139):
-- For `SELECT q.f1 FROM (SELECT 1 AS b.f1) AS q CROSS JOIN (SELECT 2 AS q.f1) AS r`, the
-- user clearly intends the alias-qualified `q.f1` to mean "f1 from the table aliased q",
-- which under the new setting resolves to q's `b.f1` via the short-name fallback (-> 1).
-- An earlier draft of the two-pass logic let pass 1 silently soft-fail on q's
-- alias-prefix miss instead of throwing as master does, which let r's literal `q.f1`
-- column hijack the resolution and return `2`. We fix that by preserving the
-- master-equivalent throw in pass 1 and catching it at the join-tree level so pass 2 can
-- retry — only when retry is actually allowed.
SELECT q.f1 FROM (SELECT 1 AS `b.f1`) AS q CROSS JOIN (SELECT 2 AS `q.f1`) AS r;

SELECT '-- default-off: same shape errors with master-equivalent UNKNOWN_IDENTIFIER';

-- Sanity check: with the setting off, the same alias-qualified lookup must still throw,
-- so that enabling the setting strictly *adds* successful resolutions — never silently
-- changes which column an existing successful query returns.
SET analyzer_enable_short_column_names_from_subquery = 0;
SELECT q.f1 FROM (SELECT 1 AS `b.f1`) AS q CROSS JOIN (SELECT 2 AS `q.f1`) AS r; -- { serverError UNKNOWN_IDENTIFIER }
SET analyzer_enable_short_column_names_from_subquery = 1;

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
