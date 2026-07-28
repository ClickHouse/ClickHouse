-- Tags: no-random-merge-tree-settings
-- `index_granularity` is the culprit setting: it decides whether the range handed to
-- `getMonotonicityForRange` is strictly negative, strictly positive or spans zero, so the
-- randomizer (`randint(1, 65536)`) would make these probes non-deterministic. It is pinned
-- per DDL below as well.

-- Every query prints 1 when the count read through the primary key equals the full-scan
-- ground truth from an identical `ENGINE = Memory` table.

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_cv_neg SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_neg SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_pos SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_pos SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_i8n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_i8n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_i8p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_i8p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_i64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_i64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_i64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_i64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_d32p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_d32p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_d32n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_d32n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_d64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_d64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_u64 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_u64 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_f64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_f64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_f64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_f64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_dhole SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_zr SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_zr SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_zl SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_zl SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_span SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_span SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_u32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_u32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_u8g SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_u8g SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_u8 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_u8 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_nul SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_nul SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_ip1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_ip1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_ipv SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_ipv SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_cv_ip8 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m_cv_ip8 SETTINGS ignore_drop_queries_probability = 0;

-- ---------------------------------------------------------------------------------------------
-- Case 1: strictly-negative range, signed constant, integer divisor.
-- `intDiv(-1000, a)` over ascending a = -40, -30, -20, -10 is 25, 33, 50, 100, i.e. INCREASING.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE t_cv_neg (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_neg (a Int32) ENGINE = Memory;
INSERT INTO t_cv_neg VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_neg VALUES (-40), (-30), (-20), (-10);

SELECT 'c1 intDiv neg c<0 eq', (SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_neg WHERE intDiv(toInt32(-1000), a) = 25);
SELECT 'c1 intDiv neg c<0 ge', (SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(-1000), a) >= 50) = (SELECT count() FROM m_cv_neg WHERE intDiv(toInt32(-1000), a) >= 50);
SELECT 'c1 intDiv neg c<0 le', (SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(-1000), a) <= 33) = (SELECT count() FROM m_cv_neg WHERE intDiv(toInt32(-1000), a) <= 33);
SELECT 'c1 intDiv neg c>0 eq', (SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(1000), a) = -25) = (SELECT count() FROM m_cv_neg WHERE intDiv(toInt32(1000), a) = -25);
SELECT 'c1 intDiv neg c>0 ge', (SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(1000), a) >= -50) = (SELECT count() FROM m_cv_neg WHERE intDiv(toInt32(1000), a) >= -50);
SELECT 'c1 divide neg c<0 eq', (SELECT count() FROM t_cv_neg WHERE divide(toInt32(-1000), a) = 25) = (SELECT count() FROM m_cv_neg WHERE divide(toInt32(-1000), a) = 25);
SELECT 'c1 divide neg c<0 ge', (SELECT count() FROM t_cv_neg WHERE divide(toInt32(-1000), a) >= 50) = (SELECT count() FROM m_cv_neg WHERE divide(toInt32(-1000), a) >= 50);
SELECT 'c1 divide neg c>0 eq', (SELECT count() FROM t_cv_neg WHERE divide(toInt32(1000), a) = -25) = (SELECT count() FROM m_cv_neg WHERE divide(toInt32(1000), a) = -25);

-- ---------------------------------------------------------------------------------------------
-- Case 2: strictly-positive range, signed constant (regression guard: these are correct today).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE t_cv_pos (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_pos (a Int32) ENGINE = Memory;
INSERT INTO t_cv_pos VALUES (10), (20), (30), (40);
INSERT INTO m_cv_pos VALUES (10), (20), (30), (40);

SELECT 'c2 intDiv pos c<0 eq', (SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(-1000), a) = -25) = (SELECT count() FROM m_cv_pos WHERE intDiv(toInt32(-1000), a) = -25);
SELECT 'c2 intDiv pos c<0 ge', (SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(-1000), a) >= -50) = (SELECT count() FROM m_cv_pos WHERE intDiv(toInt32(-1000), a) >= -50);
SELECT 'c2 intDiv pos c>0 eq', (SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(1000), a) = 25) = (SELECT count() FROM m_cv_pos WHERE intDiv(toInt32(1000), a) = 25);
SELECT 'c2 intDiv pos c>0 ge', (SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(1000), a) >= 50) = (SELECT count() FROM m_cv_pos WHERE intDiv(toInt32(1000), a) >= 50);
SELECT 'c2 divide pos c<0 eq', (SELECT count() FROM t_cv_pos WHERE divide(toInt32(-1000), a) = -25) = (SELECT count() FROM m_cv_pos WHERE divide(toInt32(-1000), a) = -25);
SELECT 'c2 divide pos c>0 eq', (SELECT count() FROM t_cv_pos WHERE divide(toInt32(1000), a) = 25) = (SELECT count() FROM m_cv_pos WHERE divide(toInt32(1000), a) = 25);

-- ---------------------------------------------------------------------------------------------
-- Case 3: unsigned constant dividend with its high bit set. `intDiv` casts the dividend through
-- `make_signed_t`, so `intDiv(toUInt8(200), x)` == `intDiv(toInt8(-56), x)`. BOTH range signs are
-- load-bearing: on the negative range master is accidentally correct, so a `!`-only fix breaks it.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE t_cv_i8p (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_i8p (a Int8) ENGINE = Memory;
INSERT INTO t_cv_i8p VALUES (10), (20), (30), (40);
INSERT INTO m_cv_i8p VALUES (10), (20), (30), (40);
CREATE TABLE t_cv_i8n (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_i8n (a Int8) ENGINE = Memory;
INSERT INTO t_cv_i8n VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_i8n VALUES (-40), (-30), (-20), (-10);

SELECT 'c3 u8 pos eq', (SELECT count() FROM t_cv_i8p WHERE intDiv(toUInt8(200), a) = -5) = (SELECT count() FROM m_cv_i8p WHERE intDiv(toUInt8(200), a) = -5);
SELECT 'c3 u8 pos ge', (SELECT count() FROM t_cv_i8p WHERE intDiv(toUInt8(200), a) >= -2) = (SELECT count() FROM m_cv_i8p WHERE intDiv(toUInt8(200), a) >= -2);
SELECT 'c3 u8 pos le', (SELECT count() FROM t_cv_i8p WHERE intDiv(toUInt8(200), a) <= -2) = (SELECT count() FROM m_cv_i8p WHERE intDiv(toUInt8(200), a) <= -2);
SELECT 'c3 lit pos eq', (SELECT count() FROM t_cv_i8p WHERE intDiv(200, a) = -5) = (SELECT count() FROM m_cv_i8p WHERE intDiv(200, a) = -5);
SELECT 'c3 lit pos le', (SELECT count() FROM t_cv_i8p WHERE intDiv(200, a) <= -2) = (SELECT count() FROM m_cv_i8p WHERE intDiv(200, a) <= -2);
SELECT 'c3 u8 neg eq', (SELECT count() FROM t_cv_i8n WHERE intDiv(toUInt8(200), a) = 5) = (SELECT count() FROM m_cv_i8n WHERE intDiv(toUInt8(200), a) = 5);
SELECT 'c3 u8 neg ge', (SELECT count() FROM t_cv_i8n WHERE intDiv(toUInt8(200), a) >= 2) = (SELECT count() FROM m_cv_i8n WHERE intDiv(toUInt8(200), a) >= 2);
SELECT 'c3 lit neg eq', (SELECT count() FROM t_cv_i8n WHERE intDiv(200, a) = 5) = (SELECT count() FROM m_cv_i8n WHERE intDiv(200, a) = 5);
-- `divide` never reinterprets its dividend, so the same shape must keep the raw constant sign.
SELECT 'c3 divide pos eq', (SELECT count() FROM t_cv_i8p WHERE divide(toUInt8(200), a) = 20) = (SELECT count() FROM m_cv_i8p WHERE divide(toUInt8(200), a) = 20);

-- ---------------------------------------------------------------------------------------------
-- Case 4: wider unsigned constants. The boundary is `2^(8 * width - 1)` of the dividend's own
-- width; the divisor width is irrelevant. Big-int constants must be STRING literals.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE t_cv_i64n (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_i64n (a Int64) ENGINE = Memory;
INSERT INTO t_cv_i64n VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_i64n VALUES (-40), (-30), (-20), (-10);
CREATE TABLE t_cv_i64p (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_i64p (a Int64) ENGINE = Memory;
INSERT INTO t_cv_i64p VALUES (10), (20), (30), (40);
INSERT INTO m_cv_i64p VALUES (10), (20), (30), (40);

SELECT 'c4 u64 hi pos', (SELECT count() FROM t_cv_i64p WHERE intDiv(toUInt64('9223372036854775808'), a) >= -461168601842738790) = (SELECT count() FROM m_cv_i64p WHERE intDiv(toUInt64('9223372036854775808'), a) >= -461168601842738790);
SELECT 'c4 u64 hi neg', (SELECT count() FROM t_cv_i64n WHERE intDiv(toUInt64('9223372036854775808'), a) >= 461168601842738790) = (SELECT count() FROM m_cv_i64n WHERE intDiv(toUInt64('9223372036854775808'), a) >= 461168601842738790);
SELECT 'c4 u64 lo pos', (SELECT count() FROM t_cv_i64p WHERE intDiv(toUInt64('9223372036854775807'), a) >= 461168601842738790) = (SELECT count() FROM m_cv_i64p WHERE intDiv(toUInt64('9223372036854775807'), a) >= 461168601842738790);
SELECT 'c4 u64 lo neg', (SELECT count() FROM t_cv_i64n WHERE intDiv(toUInt64('9223372036854775807'), a) >= -461168601842738790) = (SELECT count() FROM m_cv_i64n WHERE intDiv(toUInt64('9223372036854775807'), a) >= -461168601842738790);

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
-- Case 6: must-not-flip / must-stay-monotone controls.
-- ---------------------------------------------------------------------------------------------
-- Range spanning zero: the function is undefined at 0, so nothing may be claimed.
CREATE TABLE t_cv_span (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE m_cv_span (a Int32) ENGINE = Memory;
INSERT INTO t_cv_span VALUES (-20), (-10), (10), (20);
INSERT INTO m_cv_span VALUES (-20), (-10), (10), (20);

SELECT 'c6 span eq', (SELECT count() FROM t_cv_span WHERE intDiv(toInt32(-1000), a) = 50) = (SELECT count() FROM m_cv_span WHERE intDiv(toInt32(-1000), a) = 50);
SELECT 'c6 span ge', (SELECT count() FROM t_cv_span WHERE intDiv(toInt32(-1000), a) >= -50) = (SELECT count() FROM m_cv_span WHERE intDiv(toInt32(-1000), a) >= -50);

-- Zero as an ENDPOINT of the range is equally undefined, so the guards must stay STRICT. `divide`
-- yields -inf rather than throwing at 0, which is what makes the oracle evaluable here.
CREATE TABLE t_cv_zr (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE m_cv_zr (a Int32) ENGINE = Memory;
INSERT INTO t_cv_zr VALUES (-40), (-20), (-10), (0);
INSERT INTO m_cv_zr VALUES (-40), (-20), (-10), (0);
CREATE TABLE t_cv_zl (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE m_cv_zl (a Int32) ENGINE = Memory;
INSERT INTO t_cv_zl VALUES (0), (10), (20), (40);
INSERT INTO m_cv_zl VALUES (0), (10), (20), (40);

SELECT 'c6 zero right eq', (SELECT count() FROM t_cv_zr WHERE divide(toInt32(-1000), a) = 50) = (SELECT count() FROM m_cv_zr WHERE divide(toInt32(-1000), a) = 50);
SELECT 'c6 zero right ge', (SELECT count() FROM t_cv_zr WHERE divide(toInt32(-1000), a) >= 50) = (SELECT count() FROM m_cv_zr WHERE divide(toInt32(-1000), a) >= 50);
SELECT 'c6 zero right le', (SELECT count() FROM t_cv_zr WHERE divide(toInt32(-1000), a) <= 50) = (SELECT count() FROM m_cv_zr WHERE divide(toInt32(-1000), a) <= 50);
SELECT 'c6 zero left ge', (SELECT count() FROM t_cv_zl WHERE divide(toInt32(-1000), a) >= -50) = (SELECT count() FROM m_cv_zl WHERE divide(toInt32(-1000), a) >= -50);
SELECT 'c6 zero left le', (SELECT count() FROM t_cv_zl WHERE divide(toInt32(-1000), a) <= -50) = (SELECT count() FROM m_cv_zl WHERE divide(toInt32(-1000), a) <= -50);

-- Both operands unsigned: the dividend is not reinterpreted, so no flip.
CREATE TABLE t_cv_u32 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_u32 (a UInt32) ENGINE = Memory;
INSERT INTO t_cv_u32 VALUES (10), (20), (30), (40);
INSERT INTO m_cv_u32 VALUES (10), (20), (30), (40);

SELECT 'c6 both unsigned eq', (SELECT count() FROM t_cv_u32 WHERE intDiv(toUInt32(1000000), a) = 100000) = (SELECT count() FROM m_cv_u32 WHERE intDiv(toUInt32(1000000), a) = 100000);
SELECT 'c6 both unsigned ge', (SELECT count() FROM t_cv_u32 WHERE intDiv(toUInt32(1000000), a) >= 50000) = (SELECT count() FROM m_cv_u32 WHERE intDiv(toUInt32(1000000), a) >= 50000);
-- Also with the DIVIDEND's own high bit set: a UInt32 dividend >= 2^31 over an unsigned divisor is
-- still not reinterpreted, because `make_signed_t` only applies when an operand is signed.
SELECT 'c6 both unsigned hi eq', (SELECT count() FROM t_cv_u32 WHERE intDiv(toUInt32(3000000000), a) = 300000000) = (SELECT count() FROM m_cv_u32 WHERE intDiv(toUInt32(3000000000), a) = 300000000);
SELECT 'c6 both unsigned hi ge', (SELECT count() FROM t_cv_u32 WHERE intDiv(toUInt32(3000000000), a) >= 100000000) = (SELECT count() FROM m_cv_u32 WHERE intDiv(toUInt32(3000000000), a) >= 100000000);
SELECT 'c6 both unsigned hi le', (SELECT count() FROM t_cv_u32 WHERE intDiv(toUInt32(3000000000), a) <= 100000000) = (SELECT count() FROM m_cv_u32 WHERE intDiv(toUInt32(3000000000), a) <= 100000000);
-- Float divisor: computes through floating point, so the unsigned dividend never reinterprets.
SELECT 'c6 float divisor', (SELECT count() FROM t_cv_f64p WHERE intDiv(toUInt64('9223372036854775808'), a) >= 230584300921369395) = (SELECT count() FROM m_cv_f64p WHERE intDiv(toUInt64('9223372036854775808'), a) >= 230584300921369395);
SELECT 'c6 float divisor ge', (SELECT count() FROM t_cv_f64p WHERE intDiv(toUInt64('9223372036854775808'), a) >= 300000000000000000) = (SELECT count() FROM m_cv_f64p WHERE intDiv(toUInt64('9223372036854775808'), a) >= 300000000000000000);
SELECT 'c6 float divisor le', (SELECT count() FROM t_cv_f64p WHERE intDiv(toUInt64('9223372036854775808'), a) <= 300000000000000000) = (SELECT count() FROM m_cv_f64p WHERE intDiv(toUInt64('9223372036854775808'), a) <= 300000000000000000);

-- An unsigned key range crossing the divisor's own `2^(8W-1)`: the VALUES jump but the ORDER is
-- preserved (non-strict monotonicity), so these must keep pruning and must not be rejected.
CREATE TABLE t_cv_u8 (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE m_cv_u8 (a UInt8) ENGINE = Memory;
INSERT INTO t_cv_u8 VALUES (1), (2), (50), (127), (128), (200), (254), (255);
INSERT INTO m_cv_u8 VALUES (1), (2), (50), (127), (128), (200), (254), (255);

SELECT 'c6 wrap eq 1', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(-100), a) = 1) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(-100), a) = 1);
SELECT 'c6 wrap eq 100', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(-100), a) = 100) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(-100), a) = 100);
SELECT 'c6 wrap eq -100', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(-100), a) = -100) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(-100), a) = -100);
SELECT 'c6 wrap ge 50', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(-100), a) >= 50) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(-100), a) >= 50);
SELECT 'c6 wrap le -50', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(-100), a) <= -50) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(-100), a) <= -50);
SELECT 'c6 wrap eq 0', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(-100), a) = 0) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(-100), a) = 0);
SELECT 'c6 wrap pos eq 1', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(127), a) = 1) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(127), a) = 1);
SELECT 'c6 wrap pos le -1', (SELECT count() FROM t_cv_u8 WHERE intDiv(toInt8(127), a) <= -1) = (SELECT count() FROM m_cv_u8 WHERE intDiv(toInt8(127), a) <= -1);

-- Same data at granularity 2 (4 granules), so that "these still prune" is observable rather than
-- implied: with a single granule any pruning decision reads as `Granules: 1/1`.
CREATE TABLE t_cv_u8g (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2;
CREATE TABLE m_cv_u8g (a UInt8) ENGINE = Memory;
INSERT INTO t_cv_u8g VALUES (1), (2), (50), (127), (128), (200), (254), (255);
INSERT INTO m_cv_u8g VALUES (1), (2), (50), (127), (128), (200), (254), (255);

SELECT 'c6 wrap g2 ge 50', (SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) >= 50) = (SELECT count() FROM m_cv_u8g WHERE intDiv(toInt8(-100), a) >= 50);
SELECT 'c6 wrap g2 eq 1', (SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) = 1) = (SELECT count() FROM m_cv_u8g WHERE intDiv(toInt8(-100), a) = 1);
SELECT 'c6 wrap g2 le -50', (SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) <= -50) = (SELECT count() FROM m_cv_u8g WHERE intDiv(toInt8(-100), a) <= -50);

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
-- Case 9: non-numeric operands. `IPv4`/`IPv6` are substituted with `UInt32`/`UInt128`, and their
-- `Field` cannot be compared with a numeric zero. Before this change every row below raised
-- `Code: 169 BAD_TYPE_OF_FIELD` during key analysis, on a valid query. There are TWO such
-- comparisons, reached at different granularities, so both are exercised.
-- ---------------------------------------------------------------------------------------------
-- (i) IP as the CONSTANT, numeric key.
SELECT 'c9i ipv4 const neg', (SELECT count() FROM t_cv_neg WHERE intDiv(toIPv4('192.168.0.1'), a) = 106273177) = (SELECT count() FROM m_cv_neg WHERE intDiv(toIPv4('192.168.0.1'), a) = 106273177);
SELECT 'c9i ipv4 const span', (SELECT count() FROM t_cv_span WHERE intDiv(toIPv4('192.168.0.1'), a) = 0) = (SELECT count() FROM m_cv_span WHERE intDiv(toIPv4('192.168.0.1'), a) = 0);
SELECT 'c9i ipv6 const neg', (SELECT count() FROM t_cv_neg WHERE intDiv(toIPv6('::ffff:c0a8:1'), a) = 0) = (SELECT count() FROM m_cv_neg WHERE intDiv(toIPv6('::ffff:c0a8:1'), a) = 0);
SELECT 'c9i ipv4 divide neg', (SELECT count() FROM t_cv_neg WHERE divide(toIPv4('192.168.0.1'), a) = 0) = (SELECT count() FROM m_cv_neg WHERE divide(toIPv4('192.168.0.1'), a) = 0);

-- (ii) IP as the KEY, numeric constant. Granularity 1 gives a singleton range (the first
-- comparison); granularity 8 gives min != max (the second one). Both are load-bearing.
CREATE TABLE t_cv_ip1 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_ip1 (a IPv4) ENGINE = Memory;
INSERT INTO t_cv_ip1 VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1');
INSERT INTO m_cv_ip1 VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1');
CREATE TABLE t_cv_ip8 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE m_cv_ip8 (a IPv4) ENGINE = Memory;
INSERT INTO t_cv_ip8 VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1');
INSERT INTO m_cv_ip8 VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1');

SELECT 'c9ii ip key g1 intDiv', (SELECT count() FROM t_cv_ip1 WHERE intDiv(1, a) = 0) = (SELECT count() FROM m_cv_ip1 WHERE intDiv(1, a) = 0);
SELECT 'c9ii ip key g1 divide', (SELECT count() FROM t_cv_ip1 WHERE divide(1, a) = 0) = (SELECT count() FROM m_cv_ip1 WHERE divide(1, a) = 0);
SELECT 'c9ii ip key g8 intDiv', (SELECT count() FROM t_cv_ip8 WHERE intDiv(1, a) = 0) = (SELECT count() FROM m_cv_ip8 WHERE intDiv(1, a) = 0);
SELECT 'c9ii ip key g8 divide', (SELECT count() FROM t_cv_ip8 WHERE divide(1, a) = 0) = (SELECT count() FROM m_cv_ip8 WHERE divide(1, a) = 0);

-- (iii) Must-not-change controls: these do not throw on master and must keep their answers. The
-- gate is restricted to the division functions with a constant LEFT operand.
SELECT 'c9iii ip plus', (SELECT count() FROM t_cv_ip8 WHERE plus(1, a) = toIPv4('1.0.0.2')) = (SELECT count() FROM m_cv_ip8 WHERE plus(1, a) = toIPv4('1.0.0.2'));
SELECT 'c9iii ip var right', (SELECT count() FROM t_cv_ip8 WHERE intDiv(a, 10) = 1677721) = (SELECT count() FROM m_cv_ip8 WHERE intDiv(a, 10) = 1677721);

-- The `variable / constant` role must keep PRUNING, not merely keep answering: it reads the
-- constant's integer field and never compares an IP field against zero. 8 rows at granularity 2
-- give 4 granules, so the pruning decision is observable in the plan (case 8 below).
CREATE TABLE t_cv_ipv (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2;
CREATE TABLE m_cv_ipv (a IPv4) ENGINE = Memory;
INSERT INTO t_cv_ipv VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1'), ('5.0.0.1'), ('6.0.0.1'), ('7.0.0.1'), ('8.0.0.1');
INSERT INTO m_cv_ipv VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1'), ('5.0.0.1'), ('6.0.0.1'), ('7.0.0.1'), ('8.0.0.1');

SELECT 'c9iii ipv eq', (SELECT count() FROM t_cv_ipv WHERE intDiv(a, 10) = 1677721) = (SELECT count() FROM m_cv_ipv WHERE intDiv(a, 10) = 1677721);

-- ---------------------------------------------------------------------------------------------
-- Case 8: pruning-liveness positive controls. The fix must not silently degrade the surviving
-- shapes into "no pruning at all"; a boolean-equality row alone cannot see that.
-- ---------------------------------------------------------------------------------------------
SET explain_query_plan_default = 'legacy';

SELECT 'c8 live neg eq', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(-1000), a) = 25) WHERE explain ILIKE '%Granules: 1/4%';
SELECT 'c8 live neg ge', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(-1000), a) >= 50) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'c8 live pos eq', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(1000), a) = 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'c8 live pos ge', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(1000), a) >= 50) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'c8 live u8 flip', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_i8p WHERE intDiv(toUInt8(200), a) <= -2) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'c8 live dec/f', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_f64n WHERE divide(toDecimal32(-1000, 0), a) = 25) WHERE explain ILIKE '%Granules: 1/4%';
SELECT 'c8 live ip var', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_ipv WHERE intDiv(a, 10) = 1677721) WHERE explain ILIKE '%Granules: 1/4%';
SELECT 'c8 live wrap ge', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) >= 50) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'c8 live wrap eq', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) = 1) WHERE explain ILIKE '%Granules: 1/4%';
SELECT 'c8 live wrap le', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) <= -50) WHERE explain ILIKE '%Granules: 1/4%';

DROP TABLE t_cv_neg SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_neg SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_pos SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_pos SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_i8n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_i8n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_i8p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_i8p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_i64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_i64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_i64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_i64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_d32p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_d32p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_d32n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_d32n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_d64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_d64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_u64 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_u64 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_f64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_f64n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_f64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_f64p SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_dhole SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_zr SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_zr SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_zl SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_zl SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_span SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_span SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_u32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_u32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_u8g SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_u8g SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_u8 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_u8 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_nul SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_nul SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_ip1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_ip1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_ipv SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_ipv SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_cv_ip8 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m_cv_ip8 SETTINGS ignore_drop_queries_probability = 0;
