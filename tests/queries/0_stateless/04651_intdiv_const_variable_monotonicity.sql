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

DROP TABLE IF EXISTS t_cv_neg;
DROP TABLE IF EXISTS m_cv_neg;
DROP TABLE IF EXISTS t_cv_pos;
DROP TABLE IF EXISTS m_cv_pos;
DROP TABLE IF EXISTS t_cv_i8n;
DROP TABLE IF EXISTS m_cv_i8n;
DROP TABLE IF EXISTS t_cv_i8p;
DROP TABLE IF EXISTS m_cv_i8p;
DROP TABLE IF EXISTS t_cv_i64n;
DROP TABLE IF EXISTS m_cv_i64n;
DROP TABLE IF EXISTS t_cv_i64p;
DROP TABLE IF EXISTS m_cv_i64p;

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
-- Case 8: pruning-liveness positive controls. The fix must not silently degrade the surviving
-- shapes into "no pruning at all"; a boolean-equality row alone cannot see that.
-- ---------------------------------------------------------------------------------------------
SET explain_query_plan_default = 'legacy';

SELECT 'c8 live neg eq', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(-1000), a) = 25) WHERE explain ILIKE '%Granules: 1/4%';
SELECT 'c8 live neg ge', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_neg WHERE intDiv(toInt32(-1000), a) >= 50) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'c8 live pos eq', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(1000), a) = 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'c8 live pos ge', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_pos WHERE intDiv(toInt32(1000), a) >= 50) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'c8 live u8 flip', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_i8p WHERE intDiv(toUInt8(200), a) <= -2) WHERE explain ILIKE '%Granules: 2/4%';

DROP TABLE t_cv_neg;
DROP TABLE m_cv_neg;
DROP TABLE t_cv_pos;
DROP TABLE m_cv_pos;
DROP TABLE t_cv_i8n;
DROP TABLE m_cv_i8n;
DROP TABLE t_cv_i8p;
DROP TABLE m_cv_i8p;
DROP TABLE t_cv_i64n;
DROP TABLE m_cv_i64n;
DROP TABLE t_cv_i64p;
DROP TABLE m_cv_i64p;
