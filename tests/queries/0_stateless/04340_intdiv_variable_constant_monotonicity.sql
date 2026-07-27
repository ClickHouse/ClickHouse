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

-- A Float divisor must NOT be treated as wrapping: intDiv(UInt64, 10.0) gets an Int64 result type
-- (Float64 is signed), but DivideIntegralImpl computes it through floating point, so there is no
-- discontinuity at 2^63 and the function stays monotonic over the whole UInt64 domain. The guard must
-- gate on the divisor being a signed integer, not on the signed result type, otherwise it falsely
-- disables key/read-in-order pruning here. The first query asserts the index count matches ground truth;
-- the second asserts read-in-order still keeps intDiv in the prefix sort, so exactly one Prefix sort
-- description line appears (prints 1 when monotonic). It counts the filtered plan rows rather than a
-- fixed substring because intDiv renders as the DIV operator in the sort description.
CREATE TABLE t_intdiv_mono (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192;
INSERT INTO t_intdiv_mono SELECT number FROM numbers(1000);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, 10.0) IN (5))
     = (SELECT countIf(intDiv(a, 10.0) IN (5)) FROM t_intdiv_mono);
SELECT count()
       FROM (EXPLAIN actions = 1 SELECT a FROM t_intdiv_mono ORDER BY intDiv(a, 10.0)
             SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
       WHERE explain LIKE '%Prefix sort description%';

DROP TABLE t_intdiv_mono;

-- Mirror case: signed dividend with an unsigned constant divisor whose high bit is set. DivideIntegralImpl
-- reinterprets the divisor through make_signed_t of the wider operand, so toUInt8(200) becomes -56 and the
-- function is monotonic DEcreasing, but the raw constant compares as positive. Without deriving the sign
-- from the effective divisor the chain is reported increasing, building a reversed Range and aborting with
-- Invalid binary search result in MergeTreeSetIndex. intDiv(a, toUInt8(200)) == intDiv(a, toInt8(-56)).
CREATE TABLE t_intdiv_mono (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono SELECT number - 127 FROM numbers(255);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toUInt8(200)) IN (1, 2))
     = (SELECT countIf(intDiv(a, toUInt8(200)) IN (1, 2)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toUInt8(200)) NOT IN (0))
     = (SELECT countIf(intDiv(a, toUInt8(200)) NOT IN (0)) FROM t_intdiv_mono);
-- The reinterpreted divisor is negative, so read-in-order keeps intDiv in the prefix sort under DESC
-- (exactly one Prefix sort description line).
SELECT count()
       FROM (EXPLAIN actions = 1 SELECT a FROM t_intdiv_mono ORDER BY intDiv(a, toUInt8(200)) DESC
             SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
       WHERE explain LIKE '%Prefix sort description%';
-- High bit NOT set (toUInt8(100) stays positive): no flip, still prunes correctly.
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toUInt8(100)) IN (0, 1))
     = (SELECT countIf(intDiv(a, toUInt8(100)) IN (0, 1)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- sizeof(dividend) > sizeof(divisor): the unsigned divisor widens into the signed dividend type and stays
-- positive (no reinterpretation), so intDiv(Int16, toUInt8(200)) divides by +200 and stays increasing.
CREATE TABLE t_intdiv_mono (a Int16) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono SELECT number - 100 FROM numbers(200);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toUInt8(200)) IN (0))
     = (SELECT countIf(intDiv(a, toUInt8(200)) IN (0)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- Wider width: Int32 dividend with a UInt32 divisor whose high bit is set (3e9 reinterprets negative).
CREATE TABLE t_intdiv_mono (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192;
INSERT INTO t_intdiv_mono SELECT number - 500 FROM numbers(1000);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toUInt32(3000000000)) IN (0))
     = (SELECT countIf(intDiv(a, toUInt32(3000000000)) IN (0)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- Decimal divisor: intDiv computes in the decimal's native signed width (DecimalBinaryOperation feeds both
-- operands into DivideIntegralImpl<NativeResultType, NativeResultType>), casting the dividend into that
-- width, so a UInt64 dividend >= 2^63 reinterprets as negative just like the signed-integer-divisor case,
-- but the wrap boundary depends on the decimal width and scale rather than the dividend width. intDiv must
-- therefore not be reported monotonic for a Decimal divisor, otherwise key analysis builds a reversed Range
-- and aborts with Invalid binary search result in MergeTreeSetIndex (or silently over-prunes in release).
CREATE TABLE t_intdiv_mono (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono VALUES (9223372036854775806), (9223372036854775807), (9223372036854775808), (9223372036854775809), (18446744073709551615);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toDecimal64(1000000000000000000, 0)) IN (9))
     = (SELECT countIf(intDiv(a, toDecimal64(1000000000000000000, 0)) IN (9)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toDecimal64(1000000000000000000, 0)) NOT IN (0, 1))
     = (SELECT countIf(intDiv(a, toDecimal64(1000000000000000000, 0)) NOT IN (0, 1)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- Sibling of the above with a signed dividend and a narrower Decimal divisor: the compute width is the
-- decimal's native width (Int32 for Decimal32), so an Int64 value >= 2^31 truncates/wraps there even though
-- the dividend is signed. Same guard (no monotonicity for a Decimal divisor) keeps this correct.
CREATE TABLE t_intdiv_mono (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono VALUES (2147483646), (2147483647), (2147483648), (2147483649), (4294967296);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toDecimal32(1000000, 0)) IN (2147))
     = (SELECT countIf(intDiv(a, toDecimal32(1000000, 0)) IN (2147)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- A Decimal dividend with an integer divisor still prunes: the result stays in the standard signed path and
-- the guard only rejects a Decimal divisor, so this monotonic case keeps its pruning.
CREATE TABLE t_intdiv_mono (a Decimal64(0)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono VALUES (10), (20), (30), (40), (50);
SELECT (SELECT count() FROM t_intdiv_mono WHERE intDiv(a, toInt64(10)) IN (2))
     = (SELECT countIf(intDiv(a, toInt64(10)) IN (2)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- divide by a Decimal divisor wraps like intDiv: it takes the same DivideIntegralImpl integral path and
-- reinterprets a UInt64 dividend >= 2^63 as negative, so it must be non-monotonic. Otherwise key analysis
-- throws Invalid binary search result in MergeTreeSetIndex (aborts in debug/sanitizer) or over-prunes.
CREATE TABLE t_intdiv_mono (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono VALUES (9223372036854775806), (9223372036854775807), (9223372036854775808), (9223372036854775809), (18446744073709551615);
SELECT (SELECT count() FROM t_intdiv_mono WHERE divide(a, toDecimal64(1000000000000000000, 0)) IN (9))
     = (SELECT countIf(divide(a, toDecimal64(1000000000000000000, 0)) IN (9)) FROM t_intdiv_mono);
SELECT (SELECT count() FROM t_intdiv_mono WHERE divide(a, toDecimal64(1000000000000000000, 0)) NOT IN (0, 1))
     = (SELECT countIf(divide(a, toDecimal64(1000000000000000000, 0)) NOT IN (0, 1)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- Sibling: signed dividend, narrower Decimal divisor. The compute width is the decimal's native width, so an
-- Int64 value >= 2^31 wraps at Int32 even though the dividend is signed. Same guard keeps it correct.
CREATE TABLE t_intdiv_mono (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono VALUES (2147483646), (2147483647), (2147483648), (2147483649), (4294967296);
SELECT (SELECT count() FROM t_intdiv_mono WHERE divide(a, toDecimal32(1000000, 0)) IN (2147))
     = (SELECT countIf(divide(a, toDecimal32(1000000, 0)) IN (2147)) FROM t_intdiv_mono);

DROP TABLE t_intdiv_mono;

-- divide by a Float divisor stays monotonic (floating-point compute, no wrap): pruning must be preserved.
-- The EXPLAIN assertion checks pruning is actually enabled (a subset of granules read); the ground-truth
-- comparison alone cannot detect a disabled index because a full scan also returns the correct count.
CREATE TABLE t_intdiv_mono (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono SELECT number FROM numbers(100);
SELECT (SELECT count() FROM t_intdiv_mono WHERE divide(a, 10.0) IN (5))
     = (SELECT countIf(divide(a, 10.0) IN (5)) FROM t_intdiv_mono);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_intdiv_mono WHERE divide(a, 10.0) IN (5))
       WHERE explain LIKE '%Granules: 2/100%';

DROP TABLE t_intdiv_mono;

-- Decimal dividend with an integer divisor stays monotonic (only a Decimal divisor is rejected): pruning
-- preserved. As above, the EXPLAIN assertion confirms the index is actually used.
CREATE TABLE t_intdiv_mono (a Decimal64(0)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_intdiv_mono SELECT number FROM numbers(100);
SELECT (SELECT count() FROM t_intdiv_mono WHERE divide(a, toInt64(10)) IN (5))
     = (SELECT countIf(divide(a, toInt64(10)) IN (5)) FROM t_intdiv_mono);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_intdiv_mono WHERE divide(a, toInt64(10)) IN (5))
       WHERE explain LIKE '%Granules: 11/100%';

DROP TABLE t_intdiv_mono;
