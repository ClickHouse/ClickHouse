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
