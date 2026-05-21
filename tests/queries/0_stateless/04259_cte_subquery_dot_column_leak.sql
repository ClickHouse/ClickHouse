SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_dot_col_leak;
CREATE TABLE t_dot_col_leak (id Int) ENGINE = Memory;
INSERT INTO t_dot_col_leak VALUES (1);

WITH
    a AS (SELECT id FROM t_dot_col_leak),
    b AS (SELECT id FROM t_dot_col_leak),
    c AS (SELECT * FROM a INNER JOIN b ON a.id = b.id)
SELECT *
FROM a
LEFT JOIN c ON c.id = a.id
LEFT JOIN b ON b.id = a.id;

-- Single-part quoted identifier still finds a column whose name contains a dot.
SELECT `b.id` FROM (SELECT 1 AS `b.id`);

-- Qualified access via the subquery's own alias still works.
SELECT t.`b.id` FROM (SELECT 1 AS `b.id`) AS t;

-- Nested column access on real storage is unaffected.
DROP TABLE IF EXISTS t_nested_dot_col_leak;
CREATE TABLE t_nested_dot_col_leak (n Nested(x Int)) ENGINE = Memory;
INSERT INTO t_nested_dot_col_leak VALUES ([1]);
SELECT n.x FROM t_nested_dot_col_leak;

-- Nested subcolumn passed through a subquery is still reachable from the outer
-- scope: the guard only fires when the identifier's first part also names a
-- sibling table expression in the join tree.
SELECT n.x FROM (SELECT n.x FROM t_nested_dot_col_leak);

-- Compatibility setting: when enabled, multi-part identifiers prefer the
-- alias-prefix path over subcolumn / dotted-column lookup, restoring the
-- old-analyzer behavior for queries that error on the new analyzer.
SELECT '-- compat setting --';

DROP TABLE IF EXISTS t2_compat;
CREATE TABLE t2_compat (id Int, b Tuple(id Int)) ENGINE = Memory;
INSERT INTO t2_compat VALUES (1, (2));

-- Without the setting, b.id resolves to the subcolumn b.b.id.
SELECT b.id FROM t2_compat b;

-- With the setting, b.id prefers the alias-prefix interpretation: id column of b.
SELECT b.id FROM t2_compat b SETTINGS analyzer_compatibility_resolve_alias_prefix_over_subcolumn = 1;

-- Inline subquery (not a CTE) with the same asterisk-rename leak. Same fix path
-- as the CTE case: the dotted-subquery match in `c` is rejected in favor of
-- the alias-prefix match in `b`.
SELECT '-- inline subquery, no subcolumn --';
SELECT *
FROM t_dot_col_leak a
LEFT JOIN (SELECT * FROM t_dot_col_leak a INNER JOIN t_dot_col_leak b ON a.id = b.id) c ON c.id = a.id
LEFT JOIN t_dot_col_leak b ON b.id = a.id;

-- With a subcolumn (Tuple `b.id`) and a same-named sibling table the analyzer
-- previously raised AMBIGUOUS_IDENTIFIER at the inner JOIN; now the inner
-- ambiguity is resolved (preferring the non-leaked side) and the outer JOIN
-- runs. With the compat setting the result also matches the old analyzer.
SELECT '-- CTE leak with subcolumn (default) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) c ON c.id = a.id
LEFT JOIN t2_compat b ON b.id = a.id;

SELECT '-- CTE leak with subcolumn (compat) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) c ON c.id = a.id
LEFT JOIN t2_compat b ON b.id = a.id
SETTINGS analyzer_compatibility_resolve_alias_prefix_over_subcolumn = 1;

-- Inline subquery aliased the same as the inner JOIN's right table, with a
-- subcolumn. Previously errored with INVALID_JOIN_ON_EXPRESSION; the JOIN-time
-- preference for the alias-matching side resolves the ambiguity.
SELECT '-- inline subquery aliased b, with subcolumn (default) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) b ON a.id = b.id;

SELECT '-- inline subquery aliased b, with subcolumn (compat) --';
SELECT *
FROM t2_compat a
LEFT JOIN (SELECT * FROM t2_compat a INNER JOIN t2_compat b ON a.id = b.id) b ON a.id = b.id
SETTINGS analyzer_compatibility_resolve_alias_prefix_over_subcolumn = 1;

DROP TABLE t2_compat;
DROP TABLE t_dot_col_leak;
DROP TABLE t_nested_dot_col_leak;
