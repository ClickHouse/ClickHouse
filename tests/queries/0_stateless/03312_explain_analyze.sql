DROP TABLE IF EXISTS test;
CREATE TABLE t1(a1 UInt64, a2 UInt64, a3 UInt64, a4 UInt64) ENGINE=MergeTree ORDER BY (a1, a2, a3);
CREATE TABLE t2(b1 UInt64, b2 UInt64, b3 UInt64, b4 UInt64) ENGINE=MergeTree ORDER BY (b1, b2, b3);
INSERT INTO t1 SELECT floor(randNormal(100, 5)), floor(randNormal(10, 1)), floor(randNormal(100, 2)), floor(randNormal(100, 10)) FROM numbers(10);
INSERT INTO t2 SELECT floor(randNormal(100, 1)), floor(randNormal(10, 1)), floor(randNormal(100, 10)), floor(randNormal(100, 2)) FROM numbers(1000);



EXPLAIN ANALYZE SELECT sum(number) FROM numbers_mt(100000) GROUP BY number % 4 FORMAT Null;
DROP TABLE IF EXISTS test;
