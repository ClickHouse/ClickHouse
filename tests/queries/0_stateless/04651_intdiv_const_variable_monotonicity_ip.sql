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
DROP TABLE IF EXISTS t_cv_span;
DROP TABLE IF EXISTS m_cv_span;
DROP TABLE IF EXISTS t_cv_ip1;
DROP TABLE IF EXISTS m_cv_ip1;
DROP TABLE IF EXISTS t_cv_ip8;
DROP TABLE IF EXISTS m_cv_ip8;
DROP TABLE IF EXISTS t_cv_ipv;
DROP TABLE IF EXISTS m_cv_ipv;
DROP TABLE IF EXISTS t_cv_ipwrap4;
DROP TABLE IF EXISTS m_cv_ipwrap4;
DROP TABLE IF EXISTS t_cv_ipwrap6;
DROP TABLE IF EXISTS m_cv_ipwrap6;

-- ---------------------------------------------------------------------------------------------
-- Case 9: non-numeric operands. `IPv4`/`IPv6` are substituted with `UInt32`/`UInt128`, and their
-- `Field` cannot be compared with a numeric zero. Before this change every row below raised
-- `Code: 169 BAD_TYPE_OF_FIELD` during key analysis, on a valid query. There are TWO such
-- comparisons, reached at different granularities, so both are exercised.
-- ---------------------------------------------------------------------------------------------
-- The numeric key fixtures are the same ones the sibling files use: a strictly-negative
-- range (cases 1-4) and a range spanning zero (case 6).
CREATE TABLE t_cv_neg (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE m_cv_neg (a Int32) ENGINE = Memory;
INSERT INTO t_cv_neg VALUES (-40), (-30), (-20), (-10);
INSERT INTO m_cv_neg VALUES (-40), (-30), (-20), (-10);
CREATE TABLE t_cv_span (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE m_cv_span (a Int32) ENGINE = Memory;
INSERT INTO t_cv_span VALUES (-20), (-10), (10), (20);
INSERT INTO m_cv_span VALUES (-20), (-10), (10), (20);

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

-- IP divisors take the `variable / constant` branch. They cannot be compared directly with
-- numeric zero during key analysis, so this must decline monotonicity rather than throw.
SELECT 'c9iii ipv4 divisor intDiv', (SELECT count() FROM t_cv_ip8 WHERE intDiv(a, toIPv4('1.0.0.1')) = 0) = (SELECT count() FROM m_cv_ip8 WHERE intDiv(a, toIPv4('1.0.0.1')) = 0);
SELECT 'c9iii ipv4 divisor divide', (SELECT count() FROM t_cv_ip8 WHERE divide(a, toIPv4('1.0.0.1')) = 0) = (SELECT count() FROM m_cv_ip8 WHERE divide(a, toIPv4('1.0.0.1')) = 0);

-- The `variable / constant` role must keep PRUNING, not merely keep answering: it reads the
-- constant's integer field and never compares an IP field against zero. 8 rows at granularity 2
-- give 4 granules, so the pruning decision is observable in the plan (case 8 below).
CREATE TABLE t_cv_ipv (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2;
CREATE TABLE m_cv_ipv (a IPv4) ENGINE = Memory;
INSERT INTO t_cv_ipv VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1'), ('5.0.0.1'), ('6.0.0.1'), ('7.0.0.1'), ('8.0.0.1');
INSERT INTO m_cv_ipv VALUES ('1.0.0.1'), ('2.0.0.1'), ('3.0.0.1'), ('4.0.0.1'), ('5.0.0.1'), ('6.0.0.1'), ('7.0.0.1'), ('8.0.0.1');

SELECT 'c9iii ipv eq', (SELECT count() FROM t_cv_ipv WHERE intDiv(a, 10) = 1677721) = (SELECT count() FROM m_cv_ipv WHERE intDiv(a, 10) = 1677721);

-- `intDiv` substitutes IP keys with unsigned integers before applying a signed divisor. The
-- ranges below cross the corresponding signed wrap, so key analysis must not prune away the
-- values above the wrap.
CREATE TABLE t_cv_ipwrap4 (b UInt8, a IPv4) ENGINE = MergeTree ORDER BY (b, a) SETTINGS index_granularity = 1;
CREATE TABLE m_cv_ipwrap4 (b UInt8, a IPv4) ENGINE = Memory;
INSERT INTO t_cv_ipwrap4 VALUES (1, '127.255.255.254'), (1, '127.255.255.255'), (1, '128.0.0.0'), (1, '128.0.0.1'), (2, '200.0.0.0');
INSERT INTO m_cv_ipwrap4 VALUES (1, '127.255.255.254'), (1, '127.255.255.255'), (1, '128.0.0.0'), (1, '128.0.0.1'), (2, '200.0.0.0');
CREATE TABLE t_cv_ipwrap6 (b UInt8, a IPv6) ENGINE = MergeTree ORDER BY (b, a) SETTINGS index_granularity = 1;
CREATE TABLE m_cv_ipwrap6 (b UInt8, a IPv6) ENGINE = Memory;
INSERT INTO t_cv_ipwrap6 VALUES (1, '7fff:ffff:ffff:ffff:ffff:ffff:ffff:fffe'), (1, '7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), (1, '8000::'), (1, '8000::1'), (2, 'ffff::');
INSERT INTO m_cv_ipwrap6 VALUES (1, '7fff:ffff:ffff:ffff:ffff:ffff:ffff:fffe'), (1, '7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), (1, '8000::'), (1, '8000::1'), (2, 'ffff::');
SELECT 'c9iv ipv4 signed divisor wrap', (SELECT count() FROM t_cv_ipwrap4 WHERE b = 1 AND intDiv(a, toInt8(-2)) >= 100) = (SELECT count() FROM m_cv_ipwrap4 WHERE b = 1 AND intDiv(a, toInt8(-2)) >= 100);
SELECT 'c9iv ipv6 signed divisor wrap', (SELECT count() FROM t_cv_ipwrap6 WHERE b = 1 AND intDiv(a, toInt8(-2)) >= 100) = (SELECT count() FROM m_cv_ipwrap6 WHERE b = 1 AND intDiv(a, toInt8(-2)) >= 100);
-- A one-sided predicate reaches the null-endpoint monotonicity path. It must also reject
-- the signed-wrap discontinuity instead of pruning the rows above it.
SELECT 'c9iv ipv4 signed divisor unbounded', (SELECT count() FROM t_cv_ipwrap4 WHERE b = 1 AND intDiv(a, toInt8(-2)) < 100) = (SELECT count() FROM m_cv_ipwrap4 WHERE b = 1 AND intDiv(a, toInt8(-2)) < 100);
SELECT 'c9iv ipv6 signed divisor unbounded', (SELECT count() FROM t_cv_ipwrap6 WHERE b = 1 AND intDiv(a, toInt8(-2)) < 100) = (SELECT count() FROM m_cv_ipwrap6 WHERE b = 1 AND intDiv(a, toInt8(-2)) < 100);

-- ---------------------------------------------------------------------------------------------
-- Case 8: pruning-liveness positive controls. The fix must not silently degrade the surviving
-- shapes into "no pruning at all"; a boolean-equality row alone cannot see that.
-- ---------------------------------------------------------------------------------------------
SET explain_query_plan_default = 'legacy';

SELECT 'c8 live ip var', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cv_ipv WHERE intDiv(a, 10) = 1677721) WHERE explain ILIKE '%Granules: 1/4%';

DROP TABLE t_cv_neg;
DROP TABLE m_cv_neg;
DROP TABLE t_cv_span;
DROP TABLE m_cv_span;
DROP TABLE t_cv_ip1;
DROP TABLE m_cv_ip1;
DROP TABLE t_cv_ip8;
DROP TABLE m_cv_ip8;
DROP TABLE t_cv_ipv;
DROP TABLE m_cv_ipv;
DROP TABLE t_cv_ipwrap4;
DROP TABLE m_cv_ipwrap4;
DROP TABLE t_cv_ipwrap6;
DROP TABLE m_cv_ipwrap6;
