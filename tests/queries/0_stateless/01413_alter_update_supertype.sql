DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number FROM numbers(10);

SELECT * FROM t;

SET mutations_sync = 1;
ALTER TABLE t UPDATE x = x - 1 WHERE x % 2 = 1;

SELECT '---';
SELECT * FROM t;

DROP TABLE t;
