SET flatten_nested = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS mv;

CREATE TABLE t (x int, y int, z int) ORDER BY x;

CREATE MATERIALIZED VIEW mv ORDER BY () AS SELECT x, ([(y, z)])::Nested(y int, z int) FROM t;

INSERT INTO t VALUES (1, 2, 3);

SELECT * FROM mv;

DROP TABLE t;
DROP TABLE mv;
