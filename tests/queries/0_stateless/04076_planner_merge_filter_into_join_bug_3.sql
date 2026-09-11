SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (c1 UInt32, c2 String, c3 String) ENGINE = Memory;
CREATE TABLE t2 (c1 UInt64, c2 String, c3 String) ENGINE = Memory;

INSERT INTO t1 VALUES (1, 'x', 'x'), (2, 'y', 'y');
INSERT INTO t2 VALUES (1, 'x', 'x'), (3, 'y', 'y');

SELECT * FROM t1 INNER JOIN t2 ON t1.c3 = t2.c3 WHERE t1.c1 = t2.c1 AND t1.c2 = t2.c2;

DROP TABLE t1;
DROP TABLE t2;
