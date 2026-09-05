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

DROP TABLE IF EXISTS t_cv_span;
DROP TABLE IF EXISTS m_cv_span;
DROP TABLE IF EXISTS t_cv_zr;
DROP TABLE IF EXISTS m_cv_zr;
DROP TABLE IF EXISTS t_cv_zl;
DROP TABLE IF EXISTS m_cv_zl;
DROP TABLE IF EXISTS t_cv_u32;
DROP TABLE IF EXISTS m_cv_u32;
DROP TABLE IF EXISTS t_cv_f64p;
DROP TABLE IF EXISTS m_cv_f64p;
DROP TABLE IF EXISTS t_cv_u8;
DROP TABLE IF EXISTS m_cv_u8;
DROP TABLE IF EXISTS t_cv_u8g;
DROP TABLE IF EXISTS m_cv_u8g;

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

-- The `Float64` fixture is the same one `..._decimal_float` uses; it is recreated here
-- because the control below belongs with the other case 6 rows.
CREATE TABLE t_cv_f64p (a Float64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_f64p (a Float64) ENGINE = Memory;
INSERT INTO t_cv_f64p VALUES (10), (20), (30), (40);
INSERT INTO m_cv_f64p VALUES (10), (20), (30), (40);

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
-- Case 8: pruning-liveness positive controls. The fix must not silently degrade the surviving
-- shapes into "no pruning at all"; a boolean-equality row alone cannot see that.
-- ---------------------------------------------------------------------------------------------
SET explain_query_plan_default = 'legacy';

SELECT 'c8 live wrap ge', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) >= 50) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'c8 live wrap eq', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) = 1) WHERE explain ILIKE '%Granules: 1/4%';
SELECT 'c8 live wrap le', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_u8g WHERE intDiv(toInt8(-100), a) <= -50) WHERE explain ILIKE '%Granules: 1/4%';

DROP TABLE t_cv_span;
DROP TABLE m_cv_span;
DROP TABLE t_cv_zr;
DROP TABLE m_cv_zr;
DROP TABLE t_cv_zl;
DROP TABLE m_cv_zl;
DROP TABLE t_cv_u32;
DROP TABLE m_cv_u32;
DROP TABLE t_cv_f64p;
DROP TABLE m_cv_f64p;
DROP TABLE t_cv_u8;
DROP TABLE m_cv_u8;
DROP TABLE t_cv_u8g;
DROP TABLE m_cv_u8g;
