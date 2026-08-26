SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_dot_col_leak;
CREATE TABLE t_dot_col_leak (id Int) ENGINE = Memory;
INSERT INTO t_dot_col_leak VALUES (1);

DROP TABLE IF EXISTS t2_compat;
CREATE TABLE t2_compat (id Int, b Tuple(id Int)) ENGINE = Memory;
INSERT INTO t2_compat VALUES (1, (2));

DROP TABLE IF EXISTS t_nested_dot_col_leak;
CREATE TABLE t_nested_dot_col_leak (n Nested(x Int)) ENGINE = Memory;
INSERT INTO t_nested_dot_col_leak VALUES ([1]);

-- Single-part quoted identifier still finds a column whose name contains a dot.
SELECT `b.id` FROM (SELECT 1 AS `b.id`);

-- Qualified access via the subquery's own alias still works.
SELECT t.`b.id` FROM (SELECT 1 AS `b.id`) AS t;

-- Nested column access on real storage is unaffected.
SELECT n.x FROM t_nested_dot_col_leak;

-- Nested subcolumn passed through a subquery is reachable from the outer scope.
SELECT n.x FROM (SELECT n.x FROM t_nested_dot_col_leak);

-- Plain subcolumn access: without the setting, `b.id` against a single table
-- aliased `b` resolves to the Tuple subcolumn `b.b.id` (= 2); with the setting,
-- the alias-prefix interpretation wins and resolves to the table's `id` (= 1).
SELECT b.id FROM t2_compat b;
SELECT b.id FROM t2_compat b SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

-- ---------------------------------------------------------------------------
-- CTE / subquery leak: a `SELECT *` over an inner JOIN exposes columns like
-- `b.id` to the outer scope; when the outer JOIN also brings a sibling table
-- aliased `b`, `b.id` is ambiguous and the new analyzer raises an error.
-- The compat setting prefers the outer alias-prefix and resolves it.
-- ---------------------------------------------------------------------------

SELECT '-- CTE leak, no subcolumn (default fails) --';
WITH
    a AS (SELECT id FROM t_dot_col_leak),
    b AS (SELECT id FROM t_dot_col_leak),
    c AS (SELECT * FROM a INNER JOIN b ON a.id = b.id)
SELECT *
FROM a
LEFT JOIN c ON c.id = a.id
LEFT JOIN b ON b.id = a.id; -- { serverError AMBIGUOUS_IDENTIFIER }

SELECT '-- CTE leak, no subcolumn (compat) --';
WITH
    a AS (SELECT id FROM t_dot_col_leak),
    b AS (SELECT id FROM t_dot_col_leak),
    c AS (SELECT * FROM a INNER JOIN b ON a.id = b.id)
SELECT *
FROM a
LEFT JOIN c ON c.id = a.id
LEFT JOIN b ON b.id = a.id
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT '-- inline subquery, no subcolumn (default fails) --';
SELECT *
FROM t_dot_col_leak a
LEFT JOIN (SELECT * FROM t_dot_col_leak a INNER JOIN t_dot_col_leak b ON a.id = b.id) c ON c.id = a.id
LEFT JOIN t_dot_col_leak b ON b.id = a.id; -- { serverError AMBIGUOUS_IDENTIFIER }

SELECT '-- inline subquery, no subcolumn (compat) --';
SELECT *
FROM t_dot_col_leak a
LEFT JOIN (SELECT * FROM t_dot_col_leak a INNER JOIN t_dot_col_leak b ON a.id = b.id) c ON c.id = a.id
LEFT JOIN t_dot_col_leak b ON b.id = a.id
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

-- Same shape, but the table also has a Tuple subcolumn `b.id`, so multiple
-- resolutions exist for `b.id`. Default still fails; compat picks the outer alias.
SELECT '-- CTE leak with subcolumn (default fails) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) c ON c.id = a.id
LEFT JOIN t2_compat b ON b.id = a.id; -- { serverError AMBIGUOUS_IDENTIFIER }

SELECT '-- CTE leak with subcolumn (compat) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) c ON c.id = a.id
LEFT JOIN t2_compat b ON b.id = a.id
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

-- Inline subquery aliased `b` (same name as the inner JOIN's right table) with
-- a subcolumn: at the outer JOIN, `b.id` could come from the alias `b` or from
-- the left table's Tuple `b.id`. Default ends up with both ON keys on one
-- side; compat restricts resolution to the alias-matching side.
SELECT '-- inline subquery aliased b, with subcolumn (default fails) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) b ON a.id = b.id; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '-- inline subquery aliased b, with subcolumn (compat) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) b ON a.id = b.id
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

DROP TABLE t_dot_col_leak;
DROP TABLE t2_compat;
DROP TABLE t_nested_dot_col_leak;
