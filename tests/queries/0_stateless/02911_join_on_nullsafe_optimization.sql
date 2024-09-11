DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x Nullable(Int64), y Nullable(UInt64)) ENGINE = TinyLog;
CREATE TABLE t2 (x Nullable(Int64), y Nullable(UInt64)) ENGINE = TinyLog;

INSERT INTO t1 VALUES (1,42), (2,2), (3,3), (NULL,NULL);
INSERT INTO t2 VALUES (NULL,NULL), (2,2), (3,33), (4,42);

SET allow_experimental_analyzer = 1;

-- { echoOn }
SELECT * FROM t1 JOIN t2 ON (t1.x <=> t2.x OR (t1.x IS NULL AND t2.x IS NULL)) ORDER BY t1.x NULLS LAST;

SELECT * FROM t1 JOIN t2 ON (t1.x <=> t2.x OR t1.x IS NULL AND t1.y <=> t2.y AND t2.x IS NULL) ORDER BY t1.x NULLS LAST;

SELECT * FROM t1 JOIN t2 ON (t1.x = t2.x OR t1.x IS NULL AND t2.x IS NULL) ORDER BY t1.x;
SELECT * FROM t1 JOIN t2 ON t1.x <=> t2.x AND (t1.x = t1.y OR t1.x IS NULL AND t1.y IS NULL) ORDER BY t1.x;

SELECT * FROM t1 JOIN t2 ON (t1.x = t2.x OR t1.x IS NULL AND t2.x IS NULL) AND t1.y <=> t2.y ORDER BY t1.x NULLS LAST;

SELECT * FROM t1 JOIN t2 ON (t1.x <=> t2.x OR t1.y <=> t2.y OR (t1.x IS NULL AND t1.y IS NULL AND t2.x IS NULL AND t2.y IS NULL)) ORDER BY t1.x NULLS LAST;

SELECT * FROM t1 JOIN t2 ON (t1.x <=> t2.x OR (t1.x IS NULL AND t2.x IS NULL)) AND (t1.y == t2.y OR (t1.y IS NULL AND t2.y IS NULL)) AND COALESCE(t1.x, 0) != 2  ORDER BY t1.x NULLS LAST;

SELECT x = y OR (x IS NULL AND y IS NULL) FROM t1 ORDER BY x NULLS LAST;
-- { echoOff }

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
