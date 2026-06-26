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

SELECT '-- on/off: explicit dotted alias on a constant projection does NOT expose a short name; alias-qualified lookup against it fails on both sides';

-- Both the setting-off baseline and the setting-on path must reject this lookup. The
-- inner projection is `1 AS `b.f1`` — a constant explicitly aliased with a dotted
-- string. The user chose that exact alias name, so unlike a real qualified column
-- reference (`SELECT b.f1 FROM ... b`) there is no underlying single-component column
-- to expose as `f1`. PostgreSQL behaves the same way: `q.f1` against a column literally
-- named `b.f1` errors. The implementation skips short-name registration whenever the
-- projection is not a resolved `ColumnNode` whose `getColumnName` is a strict suffix
-- of the canonical name preceded by `.`, which keeps the alias-prefix lookup pinned to
-- the table the user named — so a sibling that happens to expose a literal `q.f1`
-- column also cannot hijack the resolution.
SELECT q.f1 FROM (SELECT 1 AS `b.f1`) AS q CROSS JOIN (SELECT 2 AS `q.f1`) AS r; -- { serverError UNKNOWN_IDENTIFIER }
SET analyzer_enable_short_column_names_from_subquery = 0;
SELECT q.f1 FROM (SELECT 1 AS `b.f1`) AS q CROSS JOIN (SELECT 2 AS `q.f1`) AS r; -- { serverError UNKNOWN_IDENTIFIER }
SET analyzer_enable_short_column_names_from_subquery = 1;

SELECT '-- on: materialized CTE source — qualifier matches the CTE name, not the temp-storage name';

-- Bot finding: a `WITH cte AS MATERIALIZED (...)` reference is rewritten into an
-- unaliased `TableNode` whose `getStorageID().getTableName()` is an auto-generated
-- `_data_<hash>`, not the user-visible CTE name. `safe_short_name`'s `TableNode` branch
-- previously compared only against the storage table name, so it skipped registration
-- for cases where the analyzer keeps the qualifier alive (here: forced by both sides
-- of an inner join having `f1`). Now we also accept `getMaterializedCTE()->cte_name`.
SET enable_materialized_cte = 1;
SET single_join_prefer_left_table = 0;
WITH src AS MATERIALIZED (SELECT 1 AS f1),
     q AS (SELECT src.f1 FROM src JOIN (SELECT 1 AS f1) AS r ON src.f1 = r.f1)
SELECT f1 FROM q;
SET single_join_prefer_left_table = 1;
SET enable_materialized_cte = 0;

-- Note: the focused db-qualified `TableNode` source case (where the canonical projection
-- name retains the `db.table.column` form because two unaliased tables share a name
-- across databases) is exercised in the sibling `.sh` test
-- `04340_short_column_names_db_table.sh`. It needs `${CLICKHOUSE_DATABASE}`-derived
-- unique database names to be parallel-safe (`DATABASE_ALREADY_EXISTS` race under the
-- flaky check), which a pure-`.sql` test can't express without `no-parallel`.

SELECT '-- on: explicit dotted alias on a resolved column does NOT expose a short name';

-- Bot finding: previously, an explicit alias like `b.f1 AS `x.f1`` was indistinguishable
-- from an analyzer-synthesized `x.f1` qualified projection name once the alias was
-- stripped. My code happily registered `f1` as a short name (matching the underlying
-- `ColumnNode::getColumnName` against the canonical suffix), which violated the
-- "unaliased" contract of the setting and could ambiguity-kill a legitimate sibling
-- short name. The fix additionally requires the canonical's qualifier prefix to match
-- the resolved column's source alias / table name — that's true for unaliased
-- `b.f1` (qualifier `b` == source alias `b`) but false for explicit `b.f1 AS `x.f1``
-- (qualifier `x` != source alias `b`).

-- Standalone explicit dotted alias: the user picked `x.f1`, only that name should resolve.
SELECT `x.f1` FROM (SELECT b.f1 AS `x.f1` FROM (SELECT 1 AS f1) AS b);
SELECT f1 FROM (SELECT b.f1 AS `x.f1` FROM (SELECT 1 AS f1) AS b); -- { serverError UNKNOWN_IDENTIFIER }

-- The dangerous sibling case: an explicit `x.f1` alias next to an unaliased `c.f1`.
-- The buggy version registered `f1` for both projections, ambiguity-cancelled them,
-- and made the perfectly fine `c.f1`'s short name unusable. With the fix, only `c.f1`
-- registers `f1`, and the legitimate short-name lookup keeps working. CROSS JOIN with
-- distinguishable values so the assertion proves `f1` resolves to `c.f1` (= 2), not
-- to the explicitly-aliased `b.f1` (= 1).
SELECT f1 FROM (
    SELECT b.f1 AS `x.f1`, c.f1
    FROM (SELECT 1 AS f1) AS b
    CROSS JOIN (SELECT 2 AS f1) AS c
);

SELECT '-- on: literal dotted column name (`f.1`) — short name is the actual column, not a wrong split';

-- Bot finding (https://github.com/ClickHouse/ClickHouse/pull/107449#discussion_r3...):
-- For a join projecting `b.\`f.1\``, the canonical projection name flattens to `b.f.1`
-- (analyzer joins identifier parts with `.`). A naive `rfind('.')` would split into
-- `b.f` / `1` and register `1` as the fallback, exposing nothing for the real column
-- `f.1`. The fix walks the underlying projection's `ColumnNode::getColumnName` to
-- recover the actual column name (`f.1`) and verifies it is a strict suffix of the
-- canonical name preceded by `.` (so canonical is exactly `<qualifier>.<column>`).

-- Canonical (dotted) name still resolves:
SELECT `b.f.1` FROM (
    SELECT b.`f.1` FROM (SELECT 1 AS `f.1`) AS a JOIN (SELECT 1 AS `f.1`) AS b ON a.`f.1` = b.`f.1`
);

-- Real short name (`f.1`) — the actual column name — resolves via the fallback:
SELECT `f.1` FROM (
    SELECT b.`f.1` FROM (SELECT 1 AS `f.1`) AS a JOIN (SELECT 1 AS `f.1`) AS b ON a.`f.1` = b.`f.1`
);

-- A naive `rfind('.')` would have registered `1` as the short name; verify that we do NOT:
SELECT `1` FROM (
    SELECT b.`f.1` FROM (SELECT 1 AS `f.1`) AS a JOIN (SELECT 1 AS `f.1`) AS b ON a.`f.1` = b.`f.1`
); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- on: pass-1 throw inside a JOIN ... USING (...) does not leave a dangling pointer';

-- Bot finding (later report): `tryResolveIdentifierFromJoin` pushes a pointer to a
-- stack-local USING-column map onto `scope.join_using_columns` before recursing into each
-- side, and pops it on normal return. When the two-pass retry catches an
-- `UNKNOWN_IDENTIFIER` thrown from a deeper alias-prefix miss, that pop would not run
-- without `SCOPE_EXIT`, and pass 2 (or later code via `tryBindIdentifierToJoinUsingColumn`)
-- would dereference a pointer to a deallocated stack frame.
--
-- Exercise the path: outer FROM is `l JOIN r USING (id)` (resolves cleanly with
-- short-name disabled because both sides have a canonical `id`), then `SELECT l.f1` in
-- the SELECT list goes through `tryResolveIdentifierFromJoin`'s push/pop lambda. The
-- alias-prefix lookup `l.f1` misses canonically (l only exposes `b.f1` and `id`), so
-- pass 1 throws, the two-pass retry catches, and pass 2 resolves `l.f1` via the
-- short-name fallback to `b.f1`. Sanitizer builds in CI exercise the SCOPE_EXIT path;
-- in non-sanitizer builds the query just succeeds and returns `1`.
WITH l AS (
    SELECT b.f1, 1 AS id
    FROM (SELECT 1 AS f1) AS a
    JOIN (SELECT 1 AS f1) AS b ON a.f1 = b.f1
)
SELECT l.f1
FROM l
JOIN (SELECT 1 AS id, 99 AS x) AS r USING (id);

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
