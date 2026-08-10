-- Three kinds of assertion: a keyed MergeTree against an ENGINE = Memory oracle (1 means key
-- analysis agrees with execution), granule counts from EXPLAIN indexes = 1, and two expected errors.

CREATE OR REPLACE TABLE ip_mono_t4 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE OR REPLACE TABLE ip_mono_m4 (a IPv4) ENGINE = Memory;
INSERT INTO ip_mono_t4 VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
INSERT INTO ip_mono_m4 VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');

-- (A) IPv4 key, signed constant divisor: the range spans 2^31, so it is not monotonic.
SELECT 'a ipv4 signed lt', (SELECT count() FROM ip_mono_t4 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM ip_mono_m4 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'a ipv4 signed gt', (SELECT count() FROM ip_mono_t4 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM ip_mono_m4 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'a ipv4 neg gt', (SELECT count() FROM ip_mono_t4 WHERE intDiv(a, toInt8(-10)) > 0) = (SELECT count() FROM ip_mono_m4 WHERE intDiv(a, toInt8(-10)) > 0);
SELECT 'a ipv4 neg lt', (SELECT count() FROM ip_mono_t4 WHERE intDiv(a, toInt8(-10)) < 0) = (SELECT count() FROM ip_mono_m4 WHERE intDiv(a, toInt8(-10)) < 0);
SELECT 'a ipv4 wide signed', (SELECT count() FROM ip_mono_t4 WHERE intDiv(a, toInt32(1000000)) < 0) = (SELECT count() FROM ip_mono_m4 WHERE intDiv(a, toInt32(1000000)) < 0);

CREATE OR REPLACE TABLE ip_mono_t6 (a IPv6) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE OR REPLACE TABLE ip_mono_m6 (a IPv6) ENGINE = Memory;
-- `::1`, `1::` and `::ffff` have wildly different raw bits and arithmetic values, so they catch a
-- byte-order-blind conversion.
INSERT INTO ip_mono_t6 VALUES ('::1'), ('1::'), ('::ffff'), ('7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), ('8000::'), ('ffff::');
INSERT INTO ip_mono_m6 VALUES ('::1'), ('1::'), ('::ffff'), ('7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), ('8000::'), ('ffff::');

-- (A) IPv6 key, signed constant divisor: same defect at 2^127.
SELECT 'a ipv6 signed lt', (SELECT count() FROM ip_mono_t6 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM ip_mono_m6 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'a ipv6 signed gt', (SELECT count() FROM ip_mono_t6 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM ip_mono_m6 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'a ipv6 neg gt', (SELECT count() FROM ip_mono_t6 WHERE intDiv(a, toInt8(-10)) > 0) = (SELECT count() FROM ip_mono_m6 WHERE intDiv(a, toInt8(-10)) > 0);
SELECT 'a ipv6 neg lt', (SELECT count() FROM ip_mono_t6 WHERE intDiv(a, toInt8(-10)) < 0) = (SELECT count() FROM ip_mono_m6 WHERE intDiv(a, toInt8(-10)) < 0);

CREATE OR REPLACE TABLE ip_mono_ti32 (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE OR REPLACE TABLE ip_mono_mi32 (a Int32) ENGINE = Memory;
INSERT INTO ip_mono_ti32 VALUES (-2000000000), (-1000000000), (1000000000), (2000000000);
INSERT INTO ip_mono_mi32 VALUES (-2000000000), (-1000000000), (1000000000), (2000000000);
CREATE OR REPLACE TABLE ip_mono_tu32 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE OR REPLACE TABLE ip_mono_mu32 (a UInt32) ENGINE = Memory;
INSERT INTO ip_mono_tu32 VALUES (16909060), (2147483647), (2147483648), (3355508993);
INSERT INTO ip_mono_mu32 VALUES (16909060), (2147483647), (2147483648), (3355508993);

-- (B) An IP constant divisor. `toIPv4('200.0.0.0')` has its high bit set, so it acts as a negative
-- divisor and flips the direction; `toIPv4('0.0.0.10')` does not.
SELECT 'b i32 ip high bit', (SELECT count() FROM ip_mono_ti32 WHERE intDiv(a, toIPv4('200.0.0.0')) = 1) = (SELECT count() FROM ip_mono_mi32 WHERE intDiv(a, toIPv4('200.0.0.0')) = 1);
SELECT 'b i32 ip low bit', (SELECT count() FROM ip_mono_ti32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM ip_mono_mi32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
SELECT 'b i32 ipv6 const', (SELECT count() FROM ip_mono_ti32 WHERE intDiv(a, toIPv6('::10')) > 0) = (SELECT count() FROM ip_mono_mi32 WHERE intDiv(a, toIPv6('::10')) > 0);
SELECT 'b u32 ip const', (SELECT count() FROM ip_mono_tu32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM ip_mono_mu32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
SELECT 'b ipv4 key ip const', (SELECT count() FROM ip_mono_t4 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM ip_mono_m4 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
SELECT 'b divide ip const', (SELECT count() FROM ip_mono_ti32 WHERE divide(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM ip_mono_mi32 WHERE divide(a, toIPv4('0.0.0.10')) > 0);
SELECT 'b divide ipv6 const', (SELECT count() FROM ip_mono_ti32 WHERE divide(a, toIPv6('::10')) > 0) = (SELECT count() FROM ip_mono_mi32 WHERE divide(a, toIPv6('::10')) > 0);

-- (B') Singleton granule ranges, which take the separate equal-endpoints path.
CREATE OR REPLACE TABLE ip_mono_ti32g1 (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO ip_mono_ti32g1 VALUES (-2000000000), (-1000000000), (1000000000), (2000000000);
SELECT 'b1 single point', (SELECT count() FROM ip_mono_ti32g1 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) = (SELECT count() FROM ip_mono_mi32 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0);
-- An all-zero IP divisor must be recognized as zero, so the query fails as a division by zero.
SELECT count() FROM ip_mono_ti32g1 WHERE intDiv(a, toIPv4('0.0.0.0')) = 0; -- { serverError ILLEGAL_DIVISION }

-- The rows above stay correct under a blanket reject, so they cannot show that pruning survives.
-- These four do: a design refusing every IP divisor reads 0 here.
SELECT 'b intdiv ipv4 const prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_ti32g1 WHERE intDiv(a, toIPv4('0.0.0.10')) > 0) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'b intdiv ipv6 const prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_ti32g1 WHERE intDiv(a, toIPv6('::10')) > 0) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'b divide ipv4 const prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_ti32g1 WHERE divide(a, toIPv4('0.0.0.10')) > 0) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'b divide ipv6 const prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_ti32g1 WHERE divide(a, toIPv6('::10')) > 0) WHERE explain ILIKE '%Granules: 3/4%';

-- (C) Must-keep-pruning controls: an unsigned constant divisor is monotonic, so a guard that
-- rejected IP dividends outright would silently delete this optimization.
CREATE OR REPLACE TABLE ip_mono_t4g1 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO ip_mono_t4g1 VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
SELECT 'c unsigned const', (SELECT count() FROM ip_mono_t4g1 WHERE intDiv(a, toUInt32(1000000)) = 16) = (SELECT count() FROM ip_mono_m4 WHERE intDiv(a, toUInt32(1000000)) = 16);
SELECT 'c unsigned const prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_t4g1 WHERE intDiv(a, toUInt32(1000000)) = 16) WHERE explain ILIKE '%Granules: 1/4%';
-- A one-sided range below 2^31 is monotonic, so it must still prune. Asserting both the bound-only
-- count (3) and the conjunction (2) is what makes this react to a blanket refusal.
CREATE OR REPLACE TABLE ip_mono_t4p (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE OR REPLACE TABLE ip_mono_m4p (a IPv4) ENGINE = Memory;
INSERT INTO ip_mono_t4p VALUES ('0.0.0.1'), ('0.0.0.2'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
INSERT INTO ip_mono_m4p VALUES ('0.0.0.1'), ('0.0.0.2'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
SELECT 'c one sided', (SELECT count() FROM ip_mono_t4p WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv4('128.0.0.0')) = (SELECT count() FROM ip_mono_m4p WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv4('128.0.0.0'));
SELECT 'c one sided bound only', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_t4p WHERE a < toIPv4('128.0.0.0')) WHERE explain ILIKE '%Granules: 3/5%';
SELECT 'c one sided prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_t4p WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv4('128.0.0.0')) WHERE explain ILIKE '%Granules: 2/5%';
-- The same pair one width up. These are the only assertions that react to a substituted IPv6 width
-- that is too narrow, which both loses pruning below 2^127 and drops rows above it.
CREATE OR REPLACE TABLE ip_mono_t6p (a IPv6) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE OR REPLACE TABLE ip_mono_m6p (a IPv6) ENGINE = Memory;
INSERT INTO ip_mono_t6p VALUES ('::1'), ('::2'), ('7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), ('8000::'), ('ffff::');
INSERT INTO ip_mono_m6p VALUES ('::1'), ('::2'), ('7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), ('8000::'), ('ffff::');
SELECT 'c ipv6 one sided', (SELECT count() FROM ip_mono_t6p WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv6('8000::')) = (SELECT count() FROM ip_mono_m6p WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv6('8000::'));
SELECT 'c ipv6 one sided bound only', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_t6p WHERE a < toIPv6('8000::')) WHERE explain ILIKE '%Granules: 3/5%';
SELECT 'c ipv6 one sided prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_t6p WHERE intDiv(a, toInt8(10)) > 0 AND a < toIPv6('8000::')) WHERE explain ILIKE '%Granules: 2/5%';
-- `divide` computes in floating point and never wraps, so it stays monotonic.
SELECT 'c divide pos', (SELECT count() FROM ip_mono_t4 WHERE divide(a, toInt8(10)) > 100000000) = (SELECT count() FROM ip_mono_m4 WHERE divide(a, toInt8(10)) > 100000000);
SELECT 'c divide neg', (SELECT count() FROM ip_mono_t4 WHERE divide(a, toInt8(-10)) < -100000000) = (SELECT count() FROM ip_mono_m4 WHERE divide(a, toInt8(-10)) < -100000000);
-- `plus`/`minus`/`multiply` over an IP key are untouched here.
SELECT 'c plus', (SELECT count() FROM ip_mono_t4 WHERE plus(a, 2) > 100) = (SELECT count() FROM ip_mono_m4 WHERE plus(a, 2) > 100);
SELECT 'c minus', (SELECT count() FROM ip_mono_t4 WHERE minus(a, 2) > 100) = (SELECT count() FROM ip_mono_m4 WHERE minus(a, 2) > 100);
SELECT 'c multiply', (SELECT count() FROM ip_mono_t4 WHERE multiply(a, 2) > 100) = (SELECT count() FROM ip_mono_m4 WHERE multiply(a, 2) > 100);

-- (C) A wrapped IP key must behave exactly like a bare one, for correctness and for pruning.
SET allow_suspicious_low_cardinality_types = 1;
CREATE OR REPLACE TABLE ip_mono_t4lc (a LowCardinality(IPv4)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE OR REPLACE TABLE ip_mono_m4lc (a LowCardinality(IPv4)) ENGINE = Memory;
INSERT INTO ip_mono_t4lc VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
INSERT INTO ip_mono_m4lc VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
SELECT 'c lc signed', (SELECT count() FROM ip_mono_t4lc WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM ip_mono_m4lc WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'c lc unsigned prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_t4lc WHERE intDiv(a, toUInt32(1000000)) = 16) WHERE explain ILIKE '%Granules: 1/4%';
CREATE OR REPLACE TABLE ip_mono_t4n (a Nullable(IPv4)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE OR REPLACE TABLE ip_mono_m4n (a Nullable(IPv4)) ENGINE = Memory;
INSERT INTO ip_mono_t4n VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
INSERT INTO ip_mono_m4n VALUES ('1.2.3.4'), ('127.255.255.255'), ('128.0.0.0'), ('200.1.1.1');
SELECT 'c nullable signed', (SELECT count() FROM ip_mono_t4n WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM ip_mono_m4n WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'c nullable unsigned prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_mono_t4n WHERE intDiv(a, toUInt32(1000000)) = 16) WHERE explain ILIKE '%Granules: 1/4%';

-- (D) Plain integer keys, which catch a normalization accidentally widened beyond the IP types.
CREATE OR REPLACE TABLE ip_mono_tu128 (a UInt128) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE OR REPLACE TABLE ip_mono_mu128 (a UInt128) ENGINE = Memory;
INSERT INTO ip_mono_tu128 VALUES (1), (79228162514264337593543950336), (65535), (170141183460469231731687303715884105727), (170141183460469231731687303715884105728), (340277174624079928635746076935438991360);
INSERT INTO ip_mono_mu128 VALUES (1), (79228162514264337593543950336), (65535), (170141183460469231731687303715884105727), (170141183460469231731687303715884105728), (340277174624079928635746076935438991360);
SELECT 'd u32 signed lt', (SELECT count() FROM ip_mono_tu32 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM ip_mono_mu32 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'd u32 signed gt', (SELECT count() FROM ip_mono_tu32 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM ip_mono_mu32 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'd u128 signed lt', (SELECT count() FROM ip_mono_tu128 WHERE intDiv(a, toInt8(10)) < 0) = (SELECT count() FROM ip_mono_mu128 WHERE intDiv(a, toInt8(10)) < 0);
SELECT 'd u128 signed gt', (SELECT count() FROM ip_mono_tu128 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM ip_mono_mu128 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'd i32 signed', (SELECT count() FROM ip_mono_ti32 WHERE intDiv(a, toInt8(10)) > 0) = (SELECT count() FROM ip_mono_mi32 WHERE intDiv(a, toInt8(10)) > 0);
SELECT 'd i32 unsigned flip', (SELECT count() FROM ip_mono_ti32 WHERE intDiv(a, toUInt8(200)) > 0) = (SELECT count() FROM ip_mono_mi32 WHERE intDiv(a, toUInt8(200)) > 0);
SELECT count() FROM ip_mono_tu32 WHERE intDiv(a, materialize(toInt8(0))) = 0; -- { serverError ILLEGAL_DIVISION }

DROP TABLE ip_mono_t4;
DROP TABLE ip_mono_m4;
DROP TABLE ip_mono_t4g1;
DROP TABLE ip_mono_t4p;
DROP TABLE ip_mono_m4p;
DROP TABLE ip_mono_t6;
DROP TABLE ip_mono_m6;
DROP TABLE ip_mono_t6p;
DROP TABLE ip_mono_m6p;
DROP TABLE ip_mono_t4lc;
DROP TABLE ip_mono_m4lc;
DROP TABLE ip_mono_t4n;
DROP TABLE ip_mono_m4n;
DROP TABLE ip_mono_ti32;
DROP TABLE ip_mono_mi32;
DROP TABLE ip_mono_ti32g1;
DROP TABLE ip_mono_tu32;
DROP TABLE ip_mono_mu32;
DROP TABLE ip_mono_tu128;
DROP TABLE ip_mono_mu128;
