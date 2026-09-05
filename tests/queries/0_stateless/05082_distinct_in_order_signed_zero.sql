-- `DISTINCT` and `LIMIT BY` group their keys by hash equality, which tells `-0.0` and `0.0` apart, as
-- `GROUP BY` does. Their in-order variants group by comparison, which does not, so the same query
-- returned one row instead of two whenever the plan happened to pick the sorted variant.

DROP TABLE IF EXISTS t_signed_zero;
DROP TABLE IF EXISTS t_signed_zero_out;

CREATE TABLE t_signed_zero (k UInt32, f Float64, a Array(Float64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_signed_zero VALUES (1, -0.0, [-0.0]), (2, 0.0, [0.0]), (3, 1.5, [1.5]);

-- The results go through a table because `-0.0` and `0.0` compare equal, so `ORDER BY f` does not
-- order them and only a canonical rendering of the bits is comparable.
CREATE TABLE t_signed_zero_out (f Float64) ENGINE = MergeTree ORDER BY tuple();

SELECT 'distinct, in order';
INSERT INTO t_signed_zero_out SELECT DISTINCT f FROM t_signed_zero ORDER BY f SETTINGS optimize_distinct_in_order = 1;
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;
TRUNCATE TABLE t_signed_zero_out;

SELECT 'distinct, by hash';
INSERT INTO t_signed_zero_out SELECT DISTINCT f FROM t_signed_zero ORDER BY f SETTINGS optimize_distinct_in_order = 0;
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;
TRUNCATE TABLE t_signed_zero_out;

SELECT 'distinct of an array of floats, in order';
INSERT INTO t_signed_zero_out SELECT a[1] FROM (SELECT DISTINCT a FROM t_signed_zero ORDER BY a SETTINGS optimize_distinct_in_order = 1);
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;
TRUNCATE TABLE t_signed_zero_out;

SELECT 'limit by, in order';
INSERT INTO t_signed_zero_out SELECT f FROM t_signed_zero ORDER BY f LIMIT 1 BY f;
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;
TRUNCATE TABLE t_signed_zero_out;

SELECT 'limit by, by hash';
INSERT INTO t_signed_zero_out SELECT f FROM t_signed_zero LIMIT 1 BY f;
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;
TRUNCATE TABLE t_signed_zero_out;

-- `GROUP BY` is the reference: it has always kept the two zeros apart.
SELECT 'group by';
INSERT INTO t_signed_zero_out SELECT f FROM t_signed_zero GROUP BY f SETTINGS optimize_aggregation_in_order = 0;
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;

-- An integer key still groups in order.
SELECT 'the sorted variant is still used for an integer key';
SELECT DISTINCT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM t_signed_zero ORDER BY k)
WHERE explain LIKE '%DistinctSorted%' SETTINGS optimize_distinct_in_order = 1;
SELECT DISTINCT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT k FROM t_signed_zero ORDER BY k LIMIT 1 BY k)
WHERE explain LIKE '%LimitBySorted%';

DROP TABLE t_signed_zero_out;
DROP TABLE t_signed_zero;
