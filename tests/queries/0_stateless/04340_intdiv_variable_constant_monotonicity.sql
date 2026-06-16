-- Regression test for intDiv(unsigned_variable, signed_constant) monotonicity in key analysis.
-- Each query prints 1 when the index-pruned count matches the full-scan ground truth.

DROP TABLE IF EXISTS t_intdiv_mono;

-- UInt64 straddling 2^63 (the original AST fuzzer shape). Must not LOGICAL_ERROR and must not over-prune.
CREATE TABLE t_intdiv_mono (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono VALUES (9223372036854775806), (9223372036854775807), (9223372036854775808), (9223372036854775809), (18446744073709551615);

SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt128(-9223372036854775807)) IN (0, 1))
     = (SELECT countIf(intDiv(a, toInt128(-9223372036854775807)) IN (0, 1)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt128(-9223372036854775807)) NOT IN (0, 1))
     = (SELECT countIf(intDiv(a, toInt128(-9223372036854775807)) NOT IN (0, 1)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, 1000000000000000000) IN (9))
     = (SELECT countIf(intDiv(a, 1000000000000000000) IN (9)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM (SELECT a, toString(a) AS b FROM t_intdiv_mono) WHERE (intDiv(a, toInt128(-9223372036854775807)), b) NOT IN ((1, 'x'), (0, 'y')))
     = (SELECT countIf((intDiv(a, toInt128(-9223372036854775807)), toString(a)) NOT IN ((1, 'x'), (0, 'y'))) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- UInt8 over the whole domain: intDiv(a, 100) is a step function whose endpoints intDiv(0,100)=0 and
-- intDiv(255,100)=0 are equal while the interior jumps, so endpoint comparison alone cannot prove
-- monotonicity. The whole-domain IN with no other key bound also exercises the unbounded (null, null) path.
CREATE TABLE t_intdiv_mono (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono SELECT number FROM numbers(256);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt16(100)) IN (1))
     = (SELECT countIf(intDiv(a, toInt16(100)) IN (1)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt16(100)) IN (0))
     = (SELECT countIf(intDiv(a, toInt16(100)) IN (0)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt16(-100)) IN (0, 1, -1))
     = (SELECT countIf(intDiv(a, toInt16(-100)) IN (0, 1, -1)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- UInt32 crossing 2^31, tested with NOT IN. NOT IN is the operator that surfaces the silent over-pruning
-- (IN returns the right count even when unpruned), and UInt32 is a mid width not otherwise covered.
-- All rows share one granule whose key range [100, 4294967295] spans the discontinuity. The endpoints
-- intDiv(100, 2e9)=0 and intDiv(4294967295->-1, 2e9)=0 are equal while the interior jumps to 1 and -1,
-- so endpoint comparison cannot prove monotonicity and the range must be reported non-monotonic.
CREATE TABLE t_intdiv_mono (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192;
INSERT INTO t_intdiv_mono VALUES (100), (2147483647), (2147483648), (4000000000), (4294967295);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt64(2000000000)) NOT IN (0))
     = (SELECT countIf(intDiv(a, toInt64(2000000000)) NOT IN (0)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt64(-2000000000)) NOT IN (0))
     = (SELECT countIf(intDiv(a, toInt64(-2000000000)) NOT IN (0)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- Sanity: a UInt range that stays below the wrap point must still prune (monotonic inference preserved).
CREATE TABLE t_intdiv_mono (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono SELECT number FROM numbers(100);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, 10) IN (3))
     = (SELECT countIf(intDiv(a, 10) IN (3)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- Sanity: signed dividend never wraps, so intDiv(Int64, constant) stays monotonic and prunes correctly.
CREATE TABLE t_intdiv_mono (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono SELECT number - 50 FROM numbers(100);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, 10) IN (0))
     = (SELECT countIf(intDiv(a, 10) IN (0)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;
