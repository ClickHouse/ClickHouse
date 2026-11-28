SET enable_analyzer = 1;
SET correlated_subqueries_substitute_equivalent_expressions = 0;

SELECT 1 FROM (SELECT 1) AS tx(c0) WHERE (SELECT tx.c0) = 1;

CREATE TABLE t0 (c0 String) ENGINE = Log();
CREATE TABLE t2 (c0 String, c1 String, c2 String) ENGINE = Log();

INSERT INTO t0 VALUES ('a'), ('b'), ('c');
INSERT INTO t2 VALUES ('a', 'x', '1'), ('b', 'y', '2'), ('d', 'z', '3');

SELECT * FROM t2 LEFT SEMI JOIN t0 ON t2.c0 = t0.c0 ORDER BY ALL;

SELECT * FROM t2 WHERE EXISTS (SELECT 1 FROM t0 WHERE t2.c0 = t0.c0) ORDER BY ALL;
