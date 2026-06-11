-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/106923
-- A subquery whose projection has two output columns with the same name backed by
-- different expressions must not corrupt values when the subquery is wrapped in an
-- outer SELECT *. Previously, the outer matcher resolved both same-named columns to a
-- single column identifier, so one value (often a constant) overwrote the other.

SET enable_analyzer = 1;

-- Original report: non-matched RIGHT JOIN row must default BOTH left-side columns,
-- including the constant one, even when wrapped in a subquery. The two queries must
-- return identical rows.
SELECT 1, * FROM (SELECT 2 AS x, 1) AS a RIGHT JOIN (SELECT 3 AS y) AS b ON y = x;
SELECT * FROM (SELECT 1, * FROM (SELECT 2 AS x, 1) AS a RIGHT JOIN (SELECT 3 AS y) AS b ON y = x);

-- FULL JOIN: the non-matched row must default the constant left column too.
SELECT * FROM (SELECT 1, * FROM (SELECT 2 AS x, 1) AS a FULL JOIN (SELECT 3 AS y) AS b ON y = x) ORDER BY 4;

-- No join needed: outer constant column collides by name with a starred subquery column.
SELECT * FROM (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`));

-- Both duplicate-named columns non-constant: still must keep both values.
SELECT * FROM (SELECT materialize(7) AS q, * FROM (SELECT 2 AS x, materialize(0) AS q));

-- An extra wrapping level must not reintroduce the collision.
SELECT * FROM (SELECT * FROM (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)));

-- Harmless duplicate names (same value referenced twice) must stay correct.
SELECT * FROM (SELECT number, number FROM numbers(2)) ORDER BY 1;

-- Same column name from different sources must stay correct (distinct identifiers).
SELECT * FROM (SELECT t1.*, t2.* FROM (SELECT 1 AS k, 7 AS v) t1 JOIN (SELECT 1 AS k, 9 AS v) t2 ON t1.k = t2.k);

-- Reading a single duplicate-named column by name still binds to the first occurrence.
SELECT `7` FROM (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`));

-- Redefinition must be preserved (https://github.com/ClickHouse/ClickHouse/issues/14739):
-- an explicit alias after `*` shadows the same-named starred column.
DROP TABLE IF EXISTS t04327;
CREATE TABLE t04327 ENGINE = Memory AS SELECT 'base' AS my_field;
SELECT my_field FROM (SELECT *, 'redefined' AS my_field FROM t04327);
SELECT my_field FROM (SELECT 'redefined' AS my_field, * FROM t04327);
SELECT my_field FROM (SELECT *, 'redefined' AS my_field FROM (SELECT * FROM t04327));
DROP TABLE t04327;

-- A pending table/CTE column alias list `... AS s(a, b, c)` renames every column to a unique
-- alias, so disambiguation must not record the stale pre-override names: the outer `SELECT *`
-- header must be the alias list, not the inner duplicate names.
SELECT * FROM (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)) AS s(a, x, b);
SELECT a, x, b FROM (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)) AS s(a, x, b);

-- A later explicit alias only shadows a same-named column produced by `*`. Two independently
-- listed same-named columns are distinct and must keep both values, even when one carries an
-- explicit alias.
SELECT * FROM (SELECT materialize(1), materialize(2) AS `1`);
SELECT * FROM (SELECT materialize(100) AS a, * FROM (SELECT materialize(1) AS a, materialize(2) AS b));

-- A union used as a subquery exposes its column names from the first branch. When the first
-- branch has duplicate names backed by different expressions, an outer `SELECT *` must keep both
-- values instead of collapsing them; the duplicate display names are preserved.
SELECT * FROM ((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`))) ORDER BY 1;
SELECT * FROM (SELECT * FROM ((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`)))) ORDER BY 1;

-- A union maps values by position across branches but names them from the first branch, so equal
-- first-branch expressions do NOT make the duplicate positions interchangeable: the `5, 6` branch
-- must stay `5, 6`, not collapse to `5, 5`.
SELECT * FROM ((SELECT number, number FROM numbers(2)) UNION ALL (SELECT 5, 6)) ORDER BY 1;

-- A nested union as the first branch of an outer union still keeps both values, and must not leak
-- the generated internal name into the outer header (the names come recursively from the first
-- branch). Both a value form and a DESCRIBE header form are checked.
SELECT * FROM (((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION ALL (SELECT 1, 2)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`))) ORDER BY 1, 2;
DESCRIBE (SELECT * FROM (((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION ALL (SELECT 1, 2)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`))));
-- Mixed union modes keep the first branch a genuine UnionNode (no flattening); the leaf first
-- branch's duplicate names are disambiguated so the outer SELECT * keeps `7, 0`, not `7, 7`.
SELECT * FROM (((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION DISTINCT (SELECT 8, 8)) UNION ALL (SELECT 9, 9)) ORDER BY 1, 2;
-- Header forms for the genuine nested-UnionNode (mixed-mode) first branch: the leaf duplicate names
-- are disambiguated through the inner union, so the outer header keeps both `7` display names (no
-- internal `7_1` leak), `COLUMNS('^7$')` selects both, and `7_1` stays unaddressable.
SELECT * FROM (((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION DISTINCT (SELECT 1, 2)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`))) ORDER BY 1, 2;
DESCRIBE (SELECT * FROM (((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION DISTINCT (SELECT 1, 2)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`))));
SELECT COLUMNS('^7$') FROM (((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION DISTINCT (SELECT 1, 2)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`))) ORDER BY 1, 2;
SELECT COLUMNS('^7_1$') FROM (((SELECT 7, * FROM (SELECT materialize(0) AS `7`)) UNION DISTINCT (SELECT 1, 2)) UNION ALL (SELECT 9, * FROM (SELECT materialize(1) AS `9`))); -- { serverError EMPTY_LIST_OF_COLUMNS_QUERIED }

-- The disambiguation renames duplicates only internally: an outer name-sensitive matcher must see
-- the original display names, so `COLUMNS('^7$')` selects BOTH `7` columns (values 7 and 0), and
-- the generated internal name `7_1` is not user-addressable.
SELECT COLUMNS('^7$') FROM (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`));
SELECT COLUMNS('^7_1$') FROM (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)); -- { serverError EMPTY_LIST_OF_COLUMNS_QUERIED }

-- A reused MATERIALIZED CTE is materialized into a temporary table whose storage columns carry the
-- (internally disambiguated) names, but the user-visible header must still show both `7` display
-- names: the generated internal name `7_1` must not leak, and a name-sensitive `COLUMNS('^7$')`
-- matcher over the CTE must match BOTH `7` columns while `7_1` stays unaddressable.
SELECT * FROM (WITH t AS MATERIALIZED (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)) SELECT * FROM t UNION ALL SELECT * FROM t) ORDER BY 1, 2, 3 SETTINGS enable_materialized_cte = 1;
DESCRIBE (WITH t AS MATERIALIZED (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)) SELECT * FROM t UNION ALL SELECT * FROM t) SETTINGS enable_materialized_cte = 1;
SELECT COLUMNS('^7$') FROM (WITH t AS MATERIALIZED (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)) SELECT * FROM t) SETTINGS enable_materialized_cte = 1;
SELECT COLUMNS('^7_1$') FROM (WITH t AS MATERIALIZED (SELECT 7, * FROM (SELECT 2 AS x, materialize(0) AS `7`)) SELECT * FROM t) SETTINGS enable_materialized_cte = 1; -- { serverError EMPTY_LIST_OF_COLUMNS_QUERIED }
