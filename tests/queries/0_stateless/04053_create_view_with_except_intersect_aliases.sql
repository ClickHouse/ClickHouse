-- Regression test: CREATE VIEW with column aliases and EXCEPT/INTERSECT
-- caused "Expected ASTSelectQuery inside ASTSelectWithUnionQuery" exception.
-- https://github.com/ClickHouse/ClickHouse/issues/100324

CREATE TABLE t_100324_0 (a Int32, b Int32) ENGINE = Memory;
CREATE TABLE t_100324_1 (c Int32, d Int32) ENGINE = Memory;

INSERT INTO t_100324_0 VALUES (1, 2), (3, 4);
INSERT INTO t_100324_1 VALUES (1, 2), (5, 6);

CREATE VIEW v_100324_except (x, y) AS SELECT a, b FROM t_100324_0 EXCEPT DISTINCT SELECT c, d FROM t_100324_1;
SELECT * FROM v_100324_except ORDER BY x;

CREATE VIEW v_100324_intersect (x, y) AS SELECT a, b FROM t_100324_0 INTERSECT DISTINCT SELECT c, d FROM t_100324_1;
SELECT * FROM v_100324_intersect;

DROP VIEW v_100324_except;
DROP VIEW v_100324_intersect;
DROP TABLE t_100324_0;
DROP TABLE t_100324_1;
