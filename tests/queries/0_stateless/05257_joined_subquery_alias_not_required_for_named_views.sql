-- Tags: need-query-parameters

-- `joined_subquery_requires_alias` requires an alias only for a table expression that has no name to qualify an
-- ambiguous column with. A parameterized view and a view inlined by `analyzer_inline_views` are named by the view,
-- like a table, so an ambiguous column of theirs is resolved as for a table (here by `single_join_prefer_left_table`)
-- instead of failing with `ALIAS_REQUIRED`.

SET joined_subquery_requires_alias = 1;

DROP TABLE IF EXISTS t_named_left;
DROP TABLE IF EXISTS t_named_right;
DROP VIEW IF EXISTS v_named;
DROP VIEW IF EXISTS pv_named;

CREATE TABLE t_named_left (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_named_right (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_named_left VALUES (1, 'left');
INSERT INTO t_named_right VALUES (1, 'right');

CREATE VIEW v_named AS SELECT * FROM t_named_right;
CREATE VIEW pv_named AS SELECT k, v FROM t_named_right WHERE k = {p:UInt32};

SELECT '-- inlined view';
SELECT k, v FROM t_named_left INNER JOIN v_named ON t_named_left.k = v_named.k SETTINGS analyzer_inline_views = 1;
SELECT k, v FROM t_named_left INNER JOIN v_named ON t_named_left.k = v_named.k SETTINGS analyzer_inline_views = 0;
SELECT v_named.v FROM t_named_left INNER JOIN v_named ON t_named_left.k = v_named.k SETTINGS analyzer_inline_views = 1;
SELECT * FROM t_named_left INNER JOIN v_named ON t_named_left.k = v_named.k SETTINGS analyzer_inline_views = 1 FORMAT TSVWithNames;

SELECT '-- parameterized view';
SELECT k, v FROM t_named_left INNER JOIN pv_named(p = 1) ON t_named_left.k = pv_named.k;
SELECT pv_named.v FROM t_named_left INNER JOIN pv_named(p = 1) ON t_named_left.k = pv_named.k;
SELECT * FROM t_named_left INNER JOIN pv_named(p = 1) ON t_named_left.k = pv_named.k FORMAT TSVWithNames;

SELECT '-- controls: a subquery and a table function have no name';
SELECT k, v FROM t_named_left INNER JOIN (SELECT * FROM t_named_right) ON t_named_left.k = 1; -- { serverError ALIAS_REQUIRED }
SELECT * FROM t_named_left INNER JOIN (SELECT * FROM t_named_right) ON t_named_left.k = 1; -- { serverError ALIAS_REQUIRED }
SELECT k, v FROM t_named_left INNER JOIN view(SELECT * FROM t_named_right) ON t_named_left.k = 1; -- { serverError ALIAS_REQUIRED }

DROP VIEW pv_named;
DROP VIEW v_named;
DROP TABLE t_named_right;
DROP TABLE t_named_left;
