DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;

SELECT 1 FROM remote('localhost', currentDatabase(), t0) AS t0
JOIN t0 t1 ON FALSE
RIGHT JOIN t0 t2 ON FALSE;

SET join_use_nulls = 1;

SELECT 1 FROM remote('localhost', currentDatabase(), t0) AS t0
JOIN t0 t1 ON FALSE
RIGHT JOIN t0 t2 ON FALSE;

DROP TABLE t0;
