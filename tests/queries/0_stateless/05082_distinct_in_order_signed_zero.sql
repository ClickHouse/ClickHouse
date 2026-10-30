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
TRUNCATE TABLE t_signed_zero_out;

-- The preliminary `DISTINCT` takes its groups from the sorting key of the table, so a float sorting key
-- reaches it without any `ORDER BY` in the query, through `optimize_read_in_order`.
CREATE TABLE t_signed_zero_float_key (f Float64) ENGINE = MergeTree ORDER BY f;
INSERT INTO t_signed_zero_float_key SELECT -0.0;
INSERT INTO t_signed_zero_float_key SELECT 0.0;
INSERT INTO t_signed_zero_float_key SELECT 1.5;

SELECT 'preliminary distinct on the float sorting key, no ORDER BY';
INSERT INTO t_signed_zero_out SELECT DISTINCT f FROM t_signed_zero_float_key;
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;
TRUNCATE TABLE t_signed_zero_out;

SELECT 'the same, with the sorted-stream transform kept out of the plan';
SELECT count() FROM (EXPLAIN PIPELINE SELECT DISTINCT f FROM t_signed_zero_float_key) WHERE explain LIKE '%DistinctSorted%';

-- A float nested in `LowCardinality` counts as a float key. Its dictionary collapses `-0.0` onto the
-- `0.0` entry today, so there is nothing to count here - the plan is what the screen has to get right.
SELECT 'a LowCardinality float key stops the sort prefix as well';
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_signed_zero_lc (lc LowCardinality(Float64)) ENGINE = MergeTree ORDER BY lc;
INSERT INTO t_signed_zero_lc SELECT -0.0;
INSERT INTO t_signed_zero_lc SELECT 0.0;
INSERT INTO t_signed_zero_lc SELECT 1.5;
SELECT count() FROM (EXPLAIN PIPELINE SELECT DISTINCT lc FROM t_signed_zero_lc) WHERE explain LIKE '%DistinctSorted%';

-- The prefix-extension shape: the query is ordered by the integer key only, and the float key extends
-- the prefix of the preliminary distinct.
CREATE TABLE t_signed_zero_pair (k UInt32, f Float64) ENGINE = MergeTree ORDER BY (k, f);
INSERT INTO t_signed_zero_pair SELECT 1, -0.0;
INSERT INTO t_signed_zero_pair SELECT 1, 0.0;
INSERT INTO t_signed_zero_pair SELECT 1, 1.5;

SELECT 'distinct over an integer key extended by a float key';
INSERT INTO t_signed_zero_out SELECT f FROM (SELECT DISTINCT k, f FROM t_signed_zero_pair ORDER BY k);
SELECT hex(reinterpretAsUInt64(f)) FROM t_signed_zero_out ORDER BY 1;
TRUNCATE TABLE t_signed_zero_out;

-- An integer key still groups in order. The number of streams, and with it the `× N` suffix of a
-- pipeline line, depends on the environment, so take the processor name only.
SELECT 'the sorted variant is still used for an integer key';
SELECT DISTINCT splitByChar(' ', trimLeft(explain))[1] FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM t_signed_zero ORDER BY k)
WHERE explain LIKE '%DistinctSorted%' SETTINGS optimize_distinct_in_order = 1;
SELECT DISTINCT splitByChar(' ', trimLeft(explain))[1] FROM (EXPLAIN PIPELINE SELECT k FROM t_signed_zero ORDER BY k LIMIT 1 BY k)
WHERE explain LIKE '%LimitBySorted%';

DROP TABLE t_signed_zero_pair;
DROP TABLE t_signed_zero_lc;
DROP TABLE t_signed_zero_float_key;
DROP TABLE t_signed_zero_out;
DROP TABLE t_signed_zero;
