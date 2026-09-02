-- Tags: no-random-merge-tree-settings
-- `index_granularity` is the culprit setting: it decides whether the range handed to
-- `getMonotonicityForRange` is strictly negative, strictly positive or spans zero, so the
-- randomizer (`randint(1, 65536)`) would make these probes non-deterministic. It is pinned
-- per DDL below as well.

-- Every query prints 1 when the count read through the primary key equals the full-scan
-- ground truth from an identical `ENGINE = Memory` table.

-- The probes are split across four files so that no single one outgrows the CI per-test time
-- limit. They share one case numbering, and each file carries the case 8 pruning-liveness rows
-- for its own fixtures. The four are `04651_intdiv_const_variable_monotonicity`
-- (cases 1-4, integer dividends), `..._decimal_float` (cases 5 and 7, `Decimal` and `Float`
-- operands), `..._controls` (case 6, must-not-flip controls) and `..._ip` (case 9, non-numeric
-- operands).

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_cv_d32p;
DROP TABLE IF EXISTS m_cv_d32p;
DROP TABLE IF EXISTS t_cv_d32n;
DROP TABLE IF EXISTS m_cv_d32n;
DROP TABLE IF EXISTS t_cv_d64p;
DROP TABLE IF EXISTS m_cv_d64p;
DROP TABLE IF EXISTS t_cv_u64;
DROP TABLE IF EXISTS m_cv_u64;
DROP TABLE IF EXISTS t_cv_f64n;
DROP TABLE IF EXISTS m_cv_f64n;
DROP TABLE IF EXISTS t_cv_f64p;
DROP TABLE IF EXISTS m_cv_f64p;
DROP TABLE IF EXISTS t_cv_dhole;
DROP TABLE IF EXISTS t_cv_lc;
DROP TABLE IF EXISTS m_cv_lc;
DROP TABLE IF EXISTS t_cv_nul;
DROP TABLE IF EXISTS m_cv_nul;

-- ---------------------------------------------------------------------------------------------
-- Case 5: a `Decimal` on either operand, with no `Float` operand, computes in the decimal's
-- native width, so nothing is claimed. A `Decimal`/`Float` pair routes through `Float64` and is
-- exempt -- those shapes are carriers of the direction defect and must be FIXED, not rejected.
-- ---------------------------------------------------------------------------------------------
-- (i) Decimal VARIABLE (the divisor)
CREATE TABLE t_cv_d32p (a Decimal32(0)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_d32p (a Decimal32(0)) ENGINE = Memory;
INSERT INTO t_cv_d32p VALUES (10), (20), (30), (40);
INSERT INTO m_cv_d32p VALUES (10), (20), (30), (40);
CREATE TABLE t_cv_d32n (a Decimal32(0)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_d32n (a Decimal32(0)) ENGINE = Memory;
INSERT INTO t_cv_d32n VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_d32n VALUES (-40), (-30), (-20), (-10);
CREATE TABLE t_cv_d64p (a Decimal64(0)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_d64p (a Decimal64(0)) ENGINE = Memory;
INSERT INTO t_cv_d64p VALUES (10), (20), (30), (40);
INSERT INTO m_cv_d64p VALUES (10), (20), (30), (40);

SELECT 'c5i d32 var eq', (SELECT count() FROM t_cv_d32p WHERE intDiv(toUInt32(3000000000), a) = -129496729) = (SELECT count() FROM m_cv_d32p WHERE intDiv(toUInt32(3000000000), a) = -129496729);
SELECT 'c5i d32 var ge', (SELECT count() FROM t_cv_d32p WHERE intDiv(toUInt32(3000000000), a) >= -64748364) = (SELECT count() FROM m_cv_d32p WHERE intDiv(toUInt32(3000000000), a) >= -64748364);
SELECT 'c5i d64 var eq', (SELECT count() FROM t_cv_d64p WHERE intDiv(toUInt64('9223372036854775808'), a) = -922337203685477580) = (SELECT count() FROM m_cv_d64p WHERE intDiv(toUInt64('9223372036854775808'), a) = -922337203685477580);
SELECT 'c5i signed neg', (SELECT count() FROM t_cv_d32n WHERE intDiv(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_d32n WHERE intDiv(toInt32(-1000), a) = 25);
SELECT 'c5i signed pos', (SELECT count() FROM t_cv_d32p WHERE intDiv(toInt32(-1000), a) = -25) = (SELECT count() FROM m_cv_d32p WHERE intDiv(toInt32(-1000), a) = -25);

-- (ii) Decimal CONSTANT: the divisor is truncated to the decimal's native width, so the quotient
-- is periodic with period `2^32` and not even monotonic.
CREATE TABLE t_cv_u64 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE m_cv_u64 (a UInt64) ENGINE = Memory;
INSERT INTO t_cv_u64 VALUES (1), (2), (10), (4294967297), (4294967307), (8589934594), (8589934604), (12884901891);
INSERT INTO m_cv_u64 VALUES (1), (2), (10), (4294967297), (4294967307), (8589934594), (8589934604), (12884901891);

SELECT 'c5ii d32 const 100', (SELECT count() FROM t_cv_u64 WHERE intDiv(toDecimal32(1000, 0), a) = 100) = (SELECT count() FROM m_cv_u64 WHERE intDiv(toDecimal32(1000, 0), a) = 100);
SELECT 'c5ii d32 const 1000', (SELECT count() FROM t_cv_u64 WHERE intDiv(toDecimal32(1000, 0), a) = 1000) = (SELECT count() FROM m_cv_u64 WHERE intDiv(toDecimal32(1000, 0), a) = 1000);
SELECT 'c5ii d32 const ge', (SELECT count() FROM t_cv_u64 WHERE intDiv(toDecimal32(1000, 0), a) >= 500) = (SELECT count() FROM m_cv_u64 WHERE intDiv(toDecimal32(1000, 0), a) >= 500);
SELECT 'c5ii divide const', (SELECT count() FROM t_cv_u64 WHERE divide(toDecimal32(1000, 0), a) = 1000) = (SELECT count() FROM m_cv_u64 WHERE divide(toDecimal32(1000, 0), a) = 1000);

-- (iii) Decimal/Float PAIR -- exempt from the rejection, and a live carrier of the direction defect.
CREATE TABLE t_cv_f64n (a Float64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_f64n (a Float64) ENGINE = Memory;
INSERT INTO t_cv_f64n VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_f64n VALUES (-40), (-30), (-20), (-10);
CREATE TABLE t_cv_f64p (a Float64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_f64p (a Float64) ENGINE = Memory;
INSERT INTO t_cv_f64p VALUES (10), (20), (30), (40);
INSERT INTO m_cv_f64p VALUES (10), (20), (30), (40);

SELECT 'c5iii dec/f divide', (SELECT count() FROM t_cv_f64n WHERE divide(toDecimal32(-1000, 0), a) = 25) = (SELECT count() FROM m_cv_f64n WHERE divide(toDecimal32(-1000, 0), a) = 25);
SELECT 'c5iii dec/f intDiv', (SELECT count() FROM t_cv_f64n WHERE intDiv(toDecimal32(-1000, 0), a) = 25) = (SELECT count() FROM m_cv_f64n WHERE intDiv(toDecimal32(-1000, 0), a) = 25);
SELECT 'c5iii dec/f pos ctl', (SELECT count() FROM t_cv_f64p WHERE divide(toDecimal32(-1000, 0), a) = -25) = (SELECT count() FROM m_cv_f64p WHERE divide(toDecimal32(-1000, 0), a) = -25);
SELECT 'c5iii f/dec mirror', (SELECT count() FROM t_cv_d32n WHERE divide(toFloat64(-1000), a) = 25) = (SELECT count() FROM m_cv_d32n WHERE divide(toFloat64(-1000), a) = 25);

-- (iv) A zero dividend does not make the rejection unnecessary: the truncation moves WHERE the
-- function is defined. With a `Decimal32` operand the divisor is truncated to `Int32`, so every
-- multiple of `2^32` divides as zero, and a range that strictly excludes 0 can still contain a
-- point where the quotient is undefined -- which is what the zero-constant guard assumes away.
-- Keying the rejection on the dividend being zero would claim monotonicity here and prune the
-- undefined row away, turning `ILLEGAL_DIVISION` into a silent answer. Asserted on the plan
-- (case 8 form) rather than on the error, because the runner randomizes settings that can make a
-- different error surface first.
CREATE TABLE t_cv_dhole (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
INSERT INTO t_cv_dhole VALUES (4294967290), (4294967293), (4294967296), (4294967300), (4294967305), (4294967310), (4294967320), (4294967330);

SELECT 'c5iv d0 hole intDiv', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_dhole WHERE intDiv(toDecimal32(0, 0), a) = 5) WHERE explain ILIKE '%Granules: 2/2%';
SELECT 'c5iv d0 hole divide', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_dhole WHERE divide(toDecimal32(0, 0), a) = 5) WHERE explain ILIKE '%Granules: 2/2%';

-- ---------------------------------------------------------------------------------------------
-- Case 7: type carriers of the direction defect.
-- ---------------------------------------------------------------------------------------------
SELECT 'c7 float64 intDiv', (SELECT count() FROM t_cv_f64n WHERE intDiv(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_f64n WHERE intDiv(toInt32(-1000), a) = 25);
SELECT 'c7 float64 divide', (SELECT count() FROM t_cv_f64n WHERE divide(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_f64n WHERE divide(toInt32(-1000), a) = 25);

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_cv_lc (a LowCardinality(Int32)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_lc (a LowCardinality(Int32)) ENGINE = Memory;
INSERT INTO t_cv_lc VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_lc VALUES (-40), (-30), (-20), (-10);

SELECT 'c7 lowcardinality', (SELECT count() FROM t_cv_lc WHERE intDiv(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_lc WHERE intDiv(toInt32(-1000), a) = 25);

CREATE TABLE t_cv_nul (a Nullable(Int32)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE m_cv_nul (a Nullable(Int32)) ENGINE = Memory;
INSERT INTO t_cv_nul VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_nul VALUES (-40), (-30), (-20), (-10);

SELECT 'c7 nullable intDiv', (SELECT count() FROM t_cv_nul WHERE intDiv(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_nul WHERE intDiv(toInt32(-1000), a) = 25);
SELECT 'c7 nullable divide', (SELECT count() FROM t_cv_nul WHERE divide(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_nul WHERE divide(toInt32(-1000), a) = 25);

-- ---------------------------------------------------------------------------------------------
-- Case 8: pruning-liveness positive controls. The fix must not silently degrade the surviving
-- shapes into "no pruning at all"; a boolean-equality row alone cannot see that.
-- ---------------------------------------------------------------------------------------------
SET explain_query_plan_default = 'legacy';

SELECT 'c8 live dec/f', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_f64n WHERE divide(toDecimal32(-1000, 0), a) = 25) WHERE explain ILIKE '%Granules: 1/4%';

DROP TABLE t_cv_d32p;
DROP TABLE m_cv_d32p;
DROP TABLE t_cv_d32n;
DROP TABLE m_cv_d32n;
DROP TABLE t_cv_d64p;
DROP TABLE m_cv_d64p;
DROP TABLE t_cv_u64;
DROP TABLE m_cv_u64;
DROP TABLE t_cv_f64n;
DROP TABLE m_cv_f64n;
DROP TABLE t_cv_f64p;
DROP TABLE m_cv_f64p;
DROP TABLE t_cv_dhole;
DROP TABLE t_cv_lc;
DROP TABLE m_cv_lc;
DROP TABLE t_cv_nul;
DROP TABLE m_cv_nul;
