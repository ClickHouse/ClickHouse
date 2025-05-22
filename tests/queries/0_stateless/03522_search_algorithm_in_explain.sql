CREATE TABLE t (a UInt8, b UInt8) ORDER BY (a, b);
INSERT INTO t VALUES (0,0);

SELECT '-- Output when the key condition is the first of the compound key';
EXPLAIN indexes = 1 SELECT * FROM t WHERE a = 0;

SELECT '';
SELECT '-- Output when the key condition is the second of the compound key';
EXPLAIN indexes = 1 SELECT * FROM t WHERE b = 0;
