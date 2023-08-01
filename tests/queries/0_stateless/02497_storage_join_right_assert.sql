DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key UInt64, a UInt64) ENGINE = Memory;
CREATE TABLE t2 (key UInt64, a UInt64) ENGINE = Join(ALL, RIGHT, key);

INSERT INTO t1 VALUES (1, 1), (2, 2);
INSERT INTO t2 VALUES (2, 2), (3, 3);

SET allow_experimental_analyzer = 0;
SELECT * FROM t1 ALL RIGHT JOIN t2 USING (key) ORDER BY key;

SET allow_experimental_analyzer = 1;
SELECT * FROM t1 ALL RIGHT JOIN t2 USING (key) ORDER BY key;
