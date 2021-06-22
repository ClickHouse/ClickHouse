DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (`a` UInt32, `b` UInt32, `c` UInt32 ) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 1, 1), (1, 1, 2), (2, 2, 2), (1, 2, 2);

SELECT DISTINCT ON (a, b) a, b, c FROM t1;

DROP TABLE IF EXISTS t1;

