-- Tags: no-random-merge-tree-settings

-- `intDiv`/`divide` substitute an `IPv4`/`IPv6` operand with `UInt32`/`UInt128`, so key analysis
-- must model the substituted type and value, not the declared one. Every row below compares a keyed
-- `MergeTree` against an `ENGINE = Memory` oracle; a `1` means key analysis agrees with execution.

DROP TABLE IF EXISTS t4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t4g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t4lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m4lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t4n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m4n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ti32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS mi32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ti32g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS tu32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS mu32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS tu128 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS mu128 SETTINGS ignore_drop_queries_probability = 0;

CREATE TABLE IF NOT EXISTS t4 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE TABLE IF NOT EXISTS m4 (a IPv4) ENGINE = Memory;
INSERT INTO t4 VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
INSERT INTO m4 VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');

-- (A) IPv4 key, signed constant divisor: the range spans 2^31, where the dividend reinterprets as
-- negative, so key analysis must not claim monotonicity. Before the fix every row here returned 0.
SELECT 'a ipv4 signed lt', (SELECT count() FROM t4 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM m4 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'a ipv4 signed gt', (SELECT count() FROM t4 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM m4 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'a ipv4 neg gt', (SELECT count() FROM t4 WHERE intDiv(a, toInt8(-10)) > 0) = (SELECT count() FROM m4 WHERE intDiv(a, toInt8(-10)) > 0);
SELECT 'a ipv4 neg lt', (SELECT count() FROM t4 WHERE intDiv(a, toInt8(-10)) < 0) = (SELECT count() FROM m4 WHERE intDiv(a, toInt8(-10)) < 0);
SELECT 'a ipv4 wide signed', (SELECT count() FROM t4 WHERE intDiv(a, toInt32(1000000)) < 0) = (SELECT count() FROM m4 WHERE intDiv(a, toInt32(1000000)) < 0);

CREATE TABLE IF NOT EXISTS t6 (a IPv6) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE TABLE IF NOT EXISTS m6 (a IPv6) ENGINE = Memory;
-- `::1`, `1::` and `::ffff` are the values whose raw underlying bits and their `UInt128` arithmetic
-- value differ by many orders of magnitude, so they catch a byte-order-blind conversion.
INSERT INTO t6 VALUES ('::1'), ('1::'), ('::ffff'), ('7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), ('8000::'), ('ffff::');
INSERT INTO m6 VALUES ('::1'), ('1::'), ('::ffff'), ('7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), ('8000::'), ('ffff::');

-- (A) IPv6 key, signed constant divisor: same defect at 2^127.
SELECT 'a ipv6 signed lt', (SELECT count() FROM t6 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM m6 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'a ipv6 signed gt', (SELECT count() FROM t6 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM m6 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'a ipv6 neg gt', (SELECT count() FROM t6 WHERE intDiv(a, toInt8(-10)) > 0) = (SELECT count() FROM m6 WHERE intDiv(a, toInt8(-10)) > 0);
SELECT 'a ipv6 neg lt', (SELECT count() FROM t6 WHERE intDiv(a, toInt8(-10)) < 0) = (SELECT count() FROM m6 WHERE intDiv(a, toInt8(-10)) < 0);

CREATE TABLE IF NOT EXISTS ti32 (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE TABLE IF NOT EXISTS mi32 (a Int32) ENGINE = Memory;
INSERT INTO ti32 VALUES (-2000000000), (-1000000000), (1000000000), (2000000000);
INSERT INTO mi32 VALUES (-2000000000), (-1000000000), (1000000000), (2000000000);
CREATE TABLE IF NOT EXISTS tu32 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE TABLE IF NOT EXISTS mu32 (a UInt32) ENGINE = Memory;
INSERT INTO tu32 VALUES (16909060), (2147483647), (2147483648), (3355508993);
INSERT INTO mu32 VALUES (16909060), (2147483647), (2147483648), (3355508993);

-- (B) An IP CONSTANT divisor: the divisor `Field` keeps its IP tag, and comparing it against zero
-- during key analysis threw `BAD_TYPE_OF_FIELD` before the fix, failing the whole query.
-- `toIPv4('200.0.0.0')` has its high bit set, so it reinterprets as a negative divisor and flips the
-- direction; `toIPv4('0.0.0.10')` does not.
SELECT 'b i32 ip high bit', (SELECT count() FROM ti32 WHERE intDiv(a, toIPv4('200.0.0.0')) = 1) = (SELECT count() FROM mi32 WHERE intDiv(a, toIPv4('200.0.0.0')) = 1);
SELECT 'b i32 ip low bit', (SELECT count() FROM ti32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM mi32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
SELECT 'b i32 ipv6 const', (SELECT count() FROM ti32 WHERE intDiv(a, toIPv6('::10')) > 0) = (SELECT count() FROM mi32 WHERE intDiv(a, toIPv6('::10')) > 0);
SELECT 'b u32 ip const', (SELECT count() FROM tu32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM mu32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
SELECT 'b ipv4 key ip const', (SELECT count() FROM t4 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM m4 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
SELECT 'b divide ip const', (SELECT count() FROM ti32 WHERE divide(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM mi32 WHERE divide(a, toIPv4('0.0.0.10')) > 0);

-- (B') The equal-endpoints branch has its own divisor-zero check and returns before the arm above.
-- With singleton granule ranges control reaches it, so it needs the same normalization.
CREATE TABLE IF NOT EXISTS ti32g1 (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO ti32g1 VALUES (-2000000000), (-1000000000), (1000000000), (2000000000);
SELECT 'b1 single point', (SELECT count() FROM ti32g1 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM mi32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
-- An all-zero IP divisor must be recognized AS zero, so the query reaches execution and reports
-- division by zero instead of failing during key analysis with `BAD_TYPE_OF_FIELD`.
SELECT count() FROM ti32g1 WHERE intDiv(a, toIPv4('0.0.0.0')) = 0; -- { serverError ILLEGAL_DIVISION }

-- (C) Must-keep-pruning controls. An UNSIGNED constant divisor never reinterprets the dividend, so
-- these are correct today and must keep pruning; a guard that rejected IP dividends outright would
-- silently delete this optimization.
CREATE TABLE IF NOT EXISTS t4g1 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t4g1 VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
SELECT 'c unsigned const', (SELECT count() FROM t4g1 WHERE intDiv(a, toUInt32(1000000)) = 16) = (SELECT count() FROM m4 WHERE intDiv(a, toUInt32(1000000)) = 16);
SELECT 'c unsigned const prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t4g1 WHERE intDiv(a, toUInt32(1000000)) = 16) WHERE explain ILIKE '%Granules: 1/4%';
-- A one-sided range that stays below 2^31 is monotonic there, so it must still prune.
SELECT 'c one sided', (SELECT count() FROM t4g1 WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv4('128.0.0.0')) = (SELECT count() FROM m4 WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv4('128.0.0.0'));
SELECT 'c one sided prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t4g1 WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv4('128.0.0.0')) WHERE explain ILIKE '%Granules: 2/4%';
-- `divide` computes in floating point and never wraps, so it stays monotonic in both directions.
SELECT 'c divide pos', (SELECT count() FROM t4 WHERE divide(a, toInt8(10)) > 100000000) = (SELECT count() FROM m4 WHERE divide(a, toInt8(10)) > 100000000);
SELECT 'c divide neg', (SELECT count() FROM t4 WHERE divide(a, toInt8(-10)) < -100000000) = (SELECT count() FROM m4 WHERE divide(a, toInt8(-10)) < -100000000);
-- `plus`/`minus`/`multiply` over an IP key decide monotonicity differently and are untouched here.
SELECT 'c plus', (SELECT count() FROM t4 WHERE plus(a, 2) > 100) = (SELECT count() FROM m4 WHERE plus(a, 2) > 100);
SELECT 'c minus', (SELECT count() FROM t4 WHERE minus(a, 2) > 100) = (SELECT count() FROM m4 WHERE minus(a, 2) > 100);
SELECT 'c multiply', (SELECT count() FROM t4 WHERE multiply(a, 2) > 100) = (SELECT count() FROM m4 WHERE multiply(a, 2) > 100);

-- (C) Wrapper matrix: the substitution runs after `Nullable`/`LowCardinality` are stripped, so a
-- wrapped IP key must behave exactly like a bare one, both for correctness and for pruning.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE IF NOT EXISTS t4lc (a LowCardinality(IPv4)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE IF NOT EXISTS m4lc (a LowCardinality(IPv4)) ENGINE = Memory;
INSERT INTO t4lc VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
INSERT INTO m4lc VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
SELECT 'c lc signed', (SELECT count() FROM t4lc WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM m4lc WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'c lc unsigned prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t4lc WHERE intDiv(a, toUInt32(1000000)) = 16) WHERE explain ILIKE '%Granules: 1/4%';
CREATE TABLE IF NOT EXISTS t4n (a Nullable(IPv4)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE IF NOT EXISTS m4n (a Nullable(IPv4)) ENGINE = Memory;
INSERT INTO t4n VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
INSERT INTO m4n VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
SELECT 'c nullable signed', (SELECT count() FROM t4n WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM m4n WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'c nullable unsigned prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t4n WHERE intDiv(a, toUInt32(1000000)) = 16) WHERE explain ILIKE '%Granules: 1/4%';

-- (D) Numeric non-regression: the same value sets on plain integer keys are correct today and must
-- stay correct, which is what catches a normalization accidentally widened beyond the IP types.
CREATE TABLE IF NOT EXISTS tu128 (a UInt128) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE TABLE IF NOT EXISTS mu128 (a UInt128) ENGINE = Memory;
INSERT INTO tu128 VALUES (1), (79228162514264337593543950336), (65535), (170141183460469231731687303715884105727), (170141183460469231731687303715884105728), (340277174624079928635746076935438991360);
INSERT INTO mu128 VALUES (1), (79228162514264337593543950336), (65535), (170141183460469231731687303715884105727), (170141183460469231731687303715884105728), (340277174624079928635746076935438991360);
SELECT 'd u32 signed lt', (SELECT count() FROM tu32 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM mu32 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'd u32 signed gt', (SELECT count() FROM tu32 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM mu32 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'd u128 signed lt', (SELECT count() FROM tu128 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM mu128 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'd u128 signed gt', (SELECT count() FROM tu128 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM mu128 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'd i32 signed', (SELECT count() FROM ti32 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM mi32 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'd i32 unsigned flip', (SELECT count() FROM ti32 WHERE intDiv(a, toUInt8(200)) > 0) = (SELECT count() FROM mi32 WHERE intDiv(a, toUInt8(200)) > 0);
SELECT count() FROM tu32 WHERE intDiv(a, materialize(toInt8(0))) = 0; -- { serverError ILLEGAL_DIVISION }

DROP TABLE t4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t4g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t4lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m4lc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t4n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m4n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ti32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE mi32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ti32g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE tu32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE mu32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE tu128 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE mu128 SETTINGS ignore_drop_queries_probability = 0;
