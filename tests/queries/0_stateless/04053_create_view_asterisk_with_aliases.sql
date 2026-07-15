-- Regression test: CREATE VIEW/MATERIALIZED VIEW with column aliases and
-- SELECT * caused "Can't set alias of * of Asterisk" exception (LOGICAL_ERROR).
-- Now gives a proper BAD_ARGUMENTS error.
-- https://github.com/ClickHouse/ClickHouse/issues/100325

-- Also covers CREATE VIEW with EXCEPT/INTERSECT (issue #100324).

CREATE TABLE t_100325 (a Int32, b String) ENGINE = Memory;
CREATE TABLE t_100325_2 (a Int32, b String) ENGINE = Memory;
INSERT INTO t_100325 VALUES (1, 'x'), (3, 'z');
INSERT INTO t_100325_2 VALUES (1, 'x'), (2, 'y');

-- Asterisk with column aliases should give a clear error
CREATE VIEW v_100325_star (x, y) AS SELECT * FROM t_100325; -- { serverError BAD_ARGUMENTS }
CREATE MATERIALIZED VIEW v_100325_mv (col) AS SELECT * FROM t_100325; -- { serverError BAD_ARGUMENTS }

-- COLUMNS matcher with column aliases should also give a clear error
CREATE VIEW v_100325_columns (x) AS SELECT COLUMNS('.*') FROM t_100325; -- { serverError BAD_ARGUMENTS }

-- EXCEPT/INTERSECT with column aliases should work
CREATE VIEW v_100325_except (x, y) AS SELECT a, b FROM t_100325 EXCEPT DISTINCT SELECT a, b FROM t_100325_2;
SELECT * FROM v_100325_except;

CREATE VIEW v_100325_intersect (x, y) AS SELECT a, b FROM t_100325 INTERSECT DISTINCT SELECT a, b FROM t_100325_2;
SELECT * FROM v_100325_intersect;

DROP VIEW v_100325_except;
DROP VIEW v_100325_intersect;
DROP TABLE t_100325;
DROP TABLE t_100325_2;
