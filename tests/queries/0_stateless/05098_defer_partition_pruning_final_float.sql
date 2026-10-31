-- https://github.com/ClickHouse/ClickHouse/issues/116981
-- Partition pruning is kept enabled for a FINAL query when the partition expression reads only
-- sorting-key columns, on the assumption that the partition key is then determined by the sorting
-- key. That holds for value identity, not for the comparator: `-0.0` compares equal to `0.0` and
-- every `NaN` bit pattern compares equal to every other, while the partition expression tells them
-- apart. Pruning then dropped the partition holding the deduplication winner.

DROP TABLE IF EXISTS t_defer_neg_zero;
CREATE TABLE t_defer_neg_zero (a UInt64, f Float64, v UInt64) ENGINE = ReplacingMergeTree(v)
PARTITION BY toString(f) ORDER BY (a, f) PRIMARY KEY (a);
INSERT INTO t_defer_neg_zero VALUES (1, 0.0, 1);
INSERT INTO t_defer_neg_zero VALUES (1, -0.0, 2);

SELECT a, f, v FROM t_defer_neg_zero FINAL ORDER BY v;
SELECT a, f, v FROM t_defer_neg_zero FINAL WHERE toString(f) = '0' ORDER BY v;
SELECT a, f, v FROM t_defer_neg_zero FINAL WHERE toString(f) = '-0' ORDER BY v;
SELECT count() FROM t_defer_neg_zero FINAL WHERE toString(f) = '0';
DROP TABLE t_defer_neg_zero;

SELECT 'NaN payloads';
DROP TABLE IF EXISTS t_defer_nan;
CREATE TABLE t_defer_nan (a UInt64, f Float64, v UInt64) ENGINE = ReplacingMergeTree(v)
PARTITION BY reinterpretAsUInt64(f) ORDER BY (a, f) PRIMARY KEY (a);
INSERT INTO t_defer_nan VALUES (1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9218868437227405313))), 1);
INSERT INTO t_defer_nan VALUES (1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560))), 2);
SELECT a, v FROM t_defer_nan FINAL ORDER BY v;
SELECT a, v FROM t_defer_nan FINAL WHERE reinterpretAsUInt64(f) = 9218868437227405313 ORDER BY v;
DROP TABLE t_defer_nan;

SELECT 'a non-float partition column still prunes';
DROP TABLE IF EXISTS t_defer_int;
CREATE TABLE t_defer_int (a UInt64, p UInt64, v UInt64) ENGINE = ReplacingMergeTree(v)
PARTITION BY p ORDER BY (a, p) PRIMARY KEY (a);
SYSTEM STOP MERGES t_defer_int;
INSERT INTO t_defer_int VALUES (1, 10, 1);
INSERT INTO t_defer_int VALUES (2, 20, 2);
SELECT a, p, v FROM t_defer_int FINAL WHERE p = 10 ORDER BY a;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a FROM t_defer_int FINAL WHERE p = 10) WHERE explain LIKE '%Parts: 1/2%';
DROP TABLE t_defer_int;

SELECT 'a float column the partition key does not read still prunes';
DROP TABLE IF EXISTS t_defer_float_unused;
CREATE TABLE t_defer_float_unused (a UInt64, p UInt64, f Float64, v UInt64) ENGINE = ReplacingMergeTree(v)
PARTITION BY p ORDER BY (a, p) PRIMARY KEY (a);
SYSTEM STOP MERGES t_defer_float_unused;
INSERT INTO t_defer_float_unused VALUES (1, 10, 0.0, 1);
INSERT INTO t_defer_float_unused VALUES (2, 20, -0.0, 2);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a FROM t_defer_float_unused FINAL WHERE p = 10) WHERE explain LIKE '%Parts: 1/2%';
DROP TABLE t_defer_float_unused;
