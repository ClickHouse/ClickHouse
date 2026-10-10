-- Tags: need-query-parameters

-- `analyzer_inline_views = 1` replaces a `VIEW` in the join tree with the view's body. The body carried no
-- name of its own, so `joined_subquery_requires_alias` (on by default) rejected a join with a view written
-- without an alias, and the query failed with 206 `ALIAS_REQUIRED` while the same query runs without
-- inlining. The inlined subquery now keeps the view's name as a table expression: it is not an alias but
-- a table name, so it needs no alias, it is usable as a qualifier, `SELECT *` qualifies columns with it,
-- and it may repeat in a `FROM` section or be spelled out as an alias, as the view itself may.

SET joined_subquery_requires_alias = 1;

DROP TABLE IF EXISTS t_inline_left;
DROP TABLE IF EXISTS t_inline_right;
DROP VIEW IF EXISTS v_inline;
DROP VIEW IF EXISTS v_inline_over_view;
DROP VIEW IF EXISTS v_inline_union;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_inline_left (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_inline_right (k UInt32, b Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_inline_left SELECT number, number FROM numbers(50);
INSERT INTO t_inline_right SELECT number, number FROM numbers(100);

CREATE VIEW v_inline AS SELECT * FROM t_inline_right;
CREATE VIEW v_inline_over_view AS SELECT * FROM v_inline;
CREATE VIEW v_inline_union AS SELECT * FROM t_inline_right UNION ALL SELECT * FROM t_inline_right;

SELECT 'a join with the view written without an alias';
SELECT count() FROM t_inline_left INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count() FROM t_inline_left INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'columns qualified by the view name';
SELECT count(), max(v_inline.b) FROM t_inline_left INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count(), max(v_inline.b) FROM t_inline_left INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'columns qualified by the database name and the view name';
SELECT count(), max({CLICKHOUSE_DATABASE:Identifier}.v_inline.b) FROM t_inline_left INNER JOIN {CLICKHOUSE_DATABASE:Identifier}.v_inline USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count(), max({CLICKHOUSE_DATABASE:Identifier}.v_inline.b) FROM t_inline_left INNER JOIN {CLICKHOUSE_DATABASE:Identifier}.v_inline USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'the same view on both sides, neither aliased';
SELECT count() FROM v_inline INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count() FROM v_inline INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'the view aliased by its own name on one side and unaliased on the other, as a table may be';
SELECT count() FROM v_inline AS v_inline INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count() FROM v_inline AS v_inline INNER JOIN v_inline USING (k) SETTINGS analyzer_inline_views = 0;
SELECT count(), max(v_inline.b) FROM t_inline_left INNER JOIN v_inline AS v_inline USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count(), max(v_inline.b) FROM t_inline_left INNER JOIN v_inline AS v_inline USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'an alias of its own still wins';
SELECT count() FROM t_inline_left INNER JOIN v_inline AS x USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count() FROM t_inline_left INNER JOIN v_inline AS x USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'a view name qualifies even next to an alias of its own, as a table name does';
SELECT count(), max(v_inline.b) FROM t_inline_left INNER JOIN v_inline AS x USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count(), max(v_inline.b) FROM t_inline_left INNER JOIN v_inline AS x USING (k) SETTINGS analyzer_inline_views = 0;
SELECT count(), max({CLICKHOUSE_DATABASE:Identifier}.v_inline.b) FROM t_inline_left INNER JOIN v_inline AS x USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count(), max({CLICKHOUSE_DATABASE:Identifier}.v_inline.b) FROM t_inline_left INNER JOIN v_inline AS x USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'a view over a view, and a view over a union';
SELECT count() FROM t_inline_left INNER JOIN v_inline_over_view USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count() FROM t_inline_left INNER JOIN v_inline_over_view USING (k) SETTINGS analyzer_inline_views = 0;
SELECT count() FROM t_inline_left INNER JOIN v_inline_union USING (k) SETTINGS analyzer_inline_views = 1;
SELECT count() FROM t_inline_left INNER JOIN v_inline_union USING (k) SETTINGS analyzer_inline_views = 0;

SELECT 'a same-named view in another database: `SELECT *` qualifies the ambiguous columns with the database and the view name';
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
-- The body refers to a table of the current database, so it needs no query parameter, which a view would store unsubstituted.
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v_inline AS SELECT k, b * 2 AS b FROM t_inline_right WHERE k < 10;
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v_inline INNER JOIN {CLICKHOUSE_DATABASE_1:Identifier}.v_inline USING (k) ORDER BY k DESC LIMIT 1 SETTINGS analyzer_inline_views = 1 FORMAT TSVWithNames;
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v_inline INNER JOIN {CLICKHOUSE_DATABASE_1:Identifier}.v_inline USING (k) ORDER BY k DESC LIMIT 1 SETTINGS analyzer_inline_views = 0 FORMAT TSVWithNames;
SELECT max({CLICKHOUSE_DATABASE:Identifier}.v_inline.b), max({CLICKHOUSE_DATABASE_1:Identifier}.v_inline.b) FROM {CLICKHOUSE_DATABASE:Identifier}.v_inline INNER JOIN {CLICKHOUSE_DATABASE_1:Identifier}.v_inline USING (k) SETTINGS analyzer_inline_views = 1;
SELECT max({CLICKHOUSE_DATABASE:Identifier}.v_inline.b), max({CLICKHOUSE_DATABASE_1:Identifier}.v_inline.b) FROM {CLICKHOUSE_DATABASE:Identifier}.v_inline INNER JOIN {CLICKHOUSE_DATABASE_1:Identifier}.v_inline USING (k) SETTINGS analyzer_inline_views = 0;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'a comma join, which the restriction covers as well';
SELECT count() FROM t_inline_left, v_inline WHERE t_inline_left.k = v_inline.k SETTINGS analyzer_inline_views = 1;
SELECT count() FROM t_inline_left, v_inline WHERE t_inline_left.k = v_inline.k SETTINGS analyzer_inline_views = 0;

DROP VIEW v_inline_union;
DROP VIEW v_inline_over_view;
DROP VIEW v_inline;
DROP TABLE t_inline_right;
DROP TABLE t_inline_left;
