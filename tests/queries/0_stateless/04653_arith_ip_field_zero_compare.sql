-- Tags: no-parallel-replicas

-- `plus`/`minus`/`multiply` substitute an `IPv4`/`IPv6` operand with `UInt32`/`UInt128`, so key
-- analysis must not compare the raw IP-tagged `Field` against a number. Every `oracle_` row compares
-- a keyed `MergeTree` against an `ENGINE = Log` oracle; a `1` means key analysis agrees with
-- execution. `prune_` rows assert the granule counts, so a change that quietly degrades the
-- optimization also fails.

DROP TABLE IF EXISTS u32g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS u32g4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ou32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ip4g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS oip4g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ip4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS oip4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ctl32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ip6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS oip6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ip6ovf SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS oip6ovf SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ip4n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS ip6be SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS oip6be SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS oip4n SETTINGS ignore_drop_queries_probability = 0;

-- Numeric key, IP constant. Granularity 1 makes every granule a min == max interval, which is the
-- only way `plus`/`minus` reach the equal-endpoints fast path.
CREATE TABLE u32g1 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE u32g4 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE TABLE ou32 (a UInt32) ENGINE = Log;
INSERT INTO u32g1 VALUES (10), (20), (30), (40);
INSERT INTO u32g4 VALUES (10), (20), (30), (40);
INSERT INTO ou32 VALUES (10), (20), (30), (40);

-- IP key, numeric constant.
CREATE TABLE ip4g1 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE oip4g1 (a IPv4) ENGINE = Log;
INSERT INTO ip4g1 VALUES (toIPv4('1.0.0.1'));
INSERT INTO oip4g1 VALUES (toIPv4('1.0.0.1'));

-- IP key with four distinct granules, plus a `UInt32` control holding the identical bit values.
CREATE TABLE ip4 (a IPv4) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE oip4 (a IPv4) ENGINE = Log;
CREATE TABLE ctl32 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO ip4 VALUES (toIPv4('1.2.3.4')), (toIPv4('2.0.0.0')), (toIPv4('3.0.0.0')), (toIPv4('200.1.1.1'));
INSERT INTO oip4 VALUES (toIPv4('1.2.3.4')), (toIPv4('2.0.0.0')), (toIPv4('3.0.0.0')), (toIPv4('200.1.1.1'));
INSERT INTO ctl32 VALUES (16909060), (33554432), (50331648), (3355508993);

-- `::ff` is chosen deliberately: its `UInt128` cast is 255 while a raw copy of the big-endian
-- underlying value is ~3.4e38, which overflows when multiplied, so this fixture distinguishes the
-- cast the arithmetic performs from a bit copy.
CREATE TABLE ip6 (a IPv6) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE oip6 (a IPv6) ENGINE = Log;
INSERT INTO ip6 VALUES (toIPv6('::1')), (toIPv6('::2')), (toIPv6('::3')), (toIPv6('::ff'));
INSERT INTO oip6 VALUES (toIPv6('::1')), (toIPv6('::2')), (toIPv6('::3')), (toIPv6('::ff'));

-- Values large enough that `a * 3` genuinely overflows `UInt128`, so the overflow check must keep
-- rejecting monotonicity here even though the endpoints are now normalized.
CREATE TABLE ip6ovf (a IPv6) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE oip6ovf (a IPv6) ENGINE = Log;
INSERT INTO ip6ovf VALUES (toIPv6('::1')), (toIPv6('8000::')), (toIPv6('ffff::')), (toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'));
INSERT INTO oip6ovf VALUES (toIPv6('::1')), (toIPv6('8000::')), (toIPv6('ffff::')), (toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'));

CREATE TABLE ip4n (a Nullable(IPv4)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE oip4n (a Nullable(IPv4)) ENGINE = Log;
INSERT INTO ip4n VALUES (toIPv4('1.2.3.4')), (toIPv4('2.0.0.0')), (toIPv4('3.0.0.0')), (toIPv4('200.1.1.1'));
INSERT INTO oip4n VALUES (toIPv4('1.2.3.4')), (toIPv4('2.0.0.0')), (toIPv4('3.0.0.0')), (toIPv4('200.1.1.1'));

-- Byte-order sensitivity. `::ff` and `::ffff` cast to the small values 255 and 65535, but a raw copy
-- of their big-endian underlying value is close to the `UInt128` maximum, so a bit copy instead of the
-- cast the arithmetic performs mis-models the overflow check and loses one more granule.
CREATE TABLE ip6be (a IPv6) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE oip6be (a IPv6) ENGINE = Log;
INSERT INTO ip6be VALUES (toIPv6('::1')), (toIPv6('::2')), (toIPv6('::ff')), (toIPv6('::ffff'));
INSERT INTO oip6be VALUES (toIPv6('::1')), (toIPv6('::2')), (toIPv6('::ff')), (toIPv6('::ffff'));

SELECT '-- IP constant over a numeric key, single-value granules';
SELECT 'oracle_plus_ip4_right_g1', (SELECT count() FROM u32g1 WHERE plus(a, toIPv4('0.0.0.2')) > 20) = (SELECT count() FROM ou32 WHERE plus(a, toIPv4('0.0.0.2')) > 20);
SELECT 'oracle_plus_ip4_left_g1', (SELECT count() FROM u32g1 WHERE plus(toIPv4('0.0.0.2'), a) > 20) = (SELECT count() FROM ou32 WHERE plus(toIPv4('0.0.0.2'), a) > 20);
SELECT 'oracle_minus_ip4_right_g1', (SELECT count() FROM u32g1 WHERE minus(a, toIPv4('0.0.0.2')) > 20) = (SELECT count() FROM ou32 WHERE minus(a, toIPv4('0.0.0.2')) > 20);
SELECT 'oracle_minus_ip4_left_g1', (SELECT count() FROM u32g1 WHERE minus(toIPv4('0.0.0.2'), a) > -20) = (SELECT count() FROM ou32 WHERE minus(toIPv4('0.0.0.2'), a) > -20);
SELECT 'oracle_multiply_ip4_right_g1', (SELECT count() FROM u32g1 WHERE multiply(a, toIPv4('0.0.0.2')) > 40) = (SELECT count() FROM ou32 WHERE multiply(a, toIPv4('0.0.0.2')) > 40);
SELECT 'oracle_multiply_ip4_left_g1', (SELECT count() FROM u32g1 WHERE multiply(toIPv4('0.0.0.2'), a) > 40) = (SELECT count() FROM ou32 WHERE multiply(toIPv4('0.0.0.2'), a) > 40);
SELECT 'oracle_plus_ip6_right_g1', (SELECT count() FROM u32g1 WHERE plus(a, toIPv6('::2')) > 20) = (SELECT count() FROM ou32 WHERE plus(a, toIPv6('::2')) > 20);
SELECT 'oracle_minus_ip6_left_g1', (SELECT count() FROM u32g1 WHERE minus(toIPv6('::2'), a) > -20) = (SELECT count() FROM ou32 WHERE minus(toIPv6('::2'), a) > -20);
SELECT 'oracle_multiply_ip6_right_g1', (SELECT count() FROM u32g1 WHERE multiply(a, toIPv6('::2')) > 40) = (SELECT count() FROM ou32 WHERE multiply(a, toIPv6('::2')) > 40);
SELECT 'oracle_multiply_ip6_left_g1', (SELECT count() FROM u32g1 WHERE multiply(toIPv6('::2'), a) > 40) = (SELECT count() FROM ou32 WHERE multiply(toIPv6('::2'), a) > 40);

-- The equal-endpoints fast path compared two different values, so it has two carriers: the key
-- endpoint, pinned by `prune_plus_ip4key_2of4` below, and the constant, pinned here. Deferring the
-- comparison keeps the interval monotonic, so the granule holding it is still discarded; a fix that
-- reported non-monotonic instead would leave every `oracle_` row green and lose only the pruning.
-- `< 25` is required: the fast path is reached for the granule whose min equals its max at the end
-- of the scanned range, which an open upper bound never excludes, so a `>` predicate cannot see the
-- difference. Each row is paired with a numeric control at the same threshold, so a count that only
-- reflects predicate selectivity cannot be mistaken for the pruning being preserved.
SELECT '-- IP constant over a numeric key must keep pruning, single-value granules';
SELECT 'oracle_plus_ip4const_right_g1', (SELECT count() FROM u32g1 WHERE plus(a, toIPv4('0.0.0.2')) < 25) = (SELECT count() FROM ou32 WHERE plus(a, toIPv4('0.0.0.2')) < 25);
SELECT 'prune_plus_ip4const_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE plus(a, toIPv4('0.0.0.2')) < 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_plus_numconst_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE plus(a, toUInt32(2)) < 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_plus_ip4const_left_g1', (SELECT count() FROM u32g1 WHERE plus(toIPv4('0.0.0.2'), a) < 25) = (SELECT count() FROM ou32 WHERE plus(toIPv4('0.0.0.2'), a) < 25);
SELECT 'prune_plus_ip4const_left_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE plus(toIPv4('0.0.0.2'), a) < 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_minus_ip4const_right_g1', (SELECT count() FROM u32g1 WHERE minus(a, toIPv4('0.0.0.2')) < 25) = (SELECT count() FROM ou32 WHERE minus(a, toIPv4('0.0.0.2')) < 25);
SELECT 'prune_minus_ip4const_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE minus(a, toIPv4('0.0.0.2')) < 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_minus_numconst_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE minus(a, toUInt32(2)) < 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_minus_ip6const_left_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE minus(toIPv6('::2'), a) > -20) WHERE explain ILIKE '%Granules: 2/4%';
-- The constant carrier only reaches the comparison when the IP operand is the right one, so these
-- are the `IPv6` shapes that actually raise `Code: 169` without the fix.
SELECT 'oracle_plus_ip6const_right_g1', (SELECT count() FROM u32g1 WHERE plus(a, toIPv6('::2')) < 25) = (SELECT count() FROM ou32 WHERE plus(a, toIPv6('::2')) < 25);
SELECT 'prune_plus_ip6const_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE plus(a, toIPv6('::2')) < 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_minus_ip6const_right_g1', (SELECT count() FROM u32g1 WHERE minus(a, toIPv6('::2')) < 25) = (SELECT count() FROM ou32 WHERE minus(a, toIPv6('::2')) < 25);
SELECT 'prune_minus_ip6const_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE minus(a, toIPv6('::2')) < 25) WHERE explain ILIKE '%Granules: 2/4%';

SELECT '-- IP constant over a numeric key, multi-value granules';
SELECT 'oracle_multiply_ip4_right_g4', (SELECT count() FROM u32g4 WHERE multiply(a, toIPv4('0.0.0.2')) = 20) = (SELECT count() FROM ou32 WHERE multiply(a, toIPv4('0.0.0.2')) = 20);
SELECT 'oracle_multiply_ip4_left_g4', (SELECT count() FROM u32g4 WHERE multiply(toIPv4('0.0.0.2'), a) = 20) = (SELECT count() FROM ou32 WHERE multiply(toIPv4('0.0.0.2'), a) = 20);
SELECT 'oracle_multiply_ip6_right_g4', (SELECT count() FROM u32g4 WHERE multiply(a, toIPv6('::2')) = 20) = (SELECT count() FROM ou32 WHERE multiply(a, toIPv6('::2')) = 20);
SELECT 'oracle_multiply_ip6_left_g4', (SELECT count() FROM u32g4 WHERE multiply(toIPv6('::2'), a) = 20) = (SELECT count() FROM ou32 WHERE multiply(toIPv6('::2'), a) = 20);
SELECT 'oracle_plus_ip4_right_g4', (SELECT count() FROM u32g4 WHERE plus(a, toIPv4('0.0.0.2')) > 20) = (SELECT count() FROM ou32 WHERE plus(a, toIPv4('0.0.0.2')) > 20);
SELECT 'oracle_minus_ip4_right_g4', (SELECT count() FROM u32g4 WHERE minus(a, toIPv4('0.0.0.2')) > 20) = (SELECT count() FROM ou32 WHERE minus(a, toIPv4('0.0.0.2')) > 20);

SELECT '-- Nullable and LowCardinality wrappers around the IP constant';
SELECT 'oracle_plus_nullable_ip4', (SELECT count() FROM u32g1 WHERE plus(a, CAST(toIPv4('0.0.0.2') AS Nullable(IPv4))) > 20) = (SELECT count() FROM ou32 WHERE plus(a, CAST(toIPv4('0.0.0.2') AS Nullable(IPv4))) > 20);
SELECT 'oracle_plus_lc_ip4', (SELECT count() FROM u32g1 WHERE plus(a, toLowCardinality(toIPv4('0.0.0.2'))) > 20) = (SELECT count() FROM ou32 WHERE plus(a, toLowCardinality(toIPv4('0.0.0.2'))) > 20);
SELECT 'oracle_multiply_nullable_ip4', (SELECT count() FROM u32g4 WHERE multiply(a, CAST(toIPv4('0.0.0.2') AS Nullable(IPv4))) = 20) = (SELECT count() FROM ou32 WHERE multiply(a, CAST(toIPv4('0.0.0.2') AS Nullable(IPv4))) = 20);
SELECT 'oracle_multiply_lc_ip4', (SELECT count() FROM u32g4 WHERE multiply(a, toLowCardinality(toIPv4('0.0.0.2'))) = 20) = (SELECT count() FROM ou32 WHERE multiply(a, toLowCardinality(toIPv4('0.0.0.2'))) = 20);
SELECT 'oracle_multiply_nullable_ip6', (SELECT count() FROM u32g4 WHERE multiply(a, CAST(toIPv6('::2') AS Nullable(IPv6))) = 20) = (SELECT count() FROM ou32 WHERE multiply(a, CAST(toIPv6('::2') AS Nullable(IPv6))) = 20);

SELECT '-- numeric constant over an IP key, single-value granules';
SELECT 'oracle_plus_const_left_ipkey', (SELECT count() FROM ip4g1 WHERE plus(1, a) > 0) = (SELECT count() FROM oip4g1 WHERE plus(1, a) > 0);
SELECT 'oracle_minus_const_left_ipkey', (SELECT count() FROM ip4g1 WHERE minus(1, a) > 0) = (SELECT count() FROM oip4g1 WHERE minus(1, a) > 0);
SELECT 'oracle_multiply_const_left_ipkey', (SELECT count() FROM ip4g1 WHERE multiply(1, a) > 0) = (SELECT count() FROM oip4g1 WHERE multiply(1, a) > 0);
SELECT 'oracle_plus_const_right_ipkey', (SELECT count() FROM ip4g1 WHERE plus(a, 1) > 0) = (SELECT count() FROM oip4g1 WHERE plus(a, 1) > 0);
SELECT 'oracle_minus_const_right_ipkey', (SELECT count() FROM ip4g1 WHERE minus(a, 1) > 0) = (SELECT count() FROM oip4g1 WHERE minus(a, 1) > 0);
SELECT 'oracle_multiply_const_right_ipkey', (SELECT count() FROM ip4g1 WHERE multiply(a, 2) > 0) = (SELECT count() FROM oip4g1 WHERE multiply(a, 2) > 0);

SELECT '-- multiplying an IP key by a numeric constant must prune like the identical numeric key';
SELECT 'oracle_multiply_ip4key', (SELECT count() FROM ip4 WHERE multiply(a, 2) > 6000000000) = (SELECT count() FROM oip4 WHERE multiply(a, 2) > 6000000000);
SELECT 'prune_multiply_ip4key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE multiply(a, 2) > 6000000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_u32key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctl32 WHERE multiply(a, 2) > 6000000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_multiply_ip6key', (SELECT count() FROM ip6 WHERE multiply(a, 2) > 100) = (SELECT count() FROM oip6 WHERE multiply(a, 2) > 100);
SELECT 'prune_multiply_ip6key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip6 WHERE multiply(a, 2) > 100) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_multiply_nullable_ip4key', (SELECT count() FROM ip4n WHERE multiply(a, 2) > 6000000000) = (SELECT count() FROM oip4n WHERE multiply(a, 2) > 6000000000);
SELECT 'prune_multiply_nullable_ip4key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4n WHERE multiply(a, 2) > 6000000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_multiply_ip6key_byteorder', (SELECT count() FROM ip6be WHERE multiply(a, 2) < 10) = (SELECT count() FROM oip6be WHERE multiply(a, 2) < 10);
SELECT 'prune_multiply_ip6key_byteorder_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip6be WHERE multiply(a, 2) < 10) WHERE explain ILIKE '%Granules: 2/4%';
-- Two IP operands at once: `getReturnTypeImplStatic` accepts them, so the constant and both endpoints
-- are IP-tagged in the same call, exercising both normalization sites together.
SELECT 'oracle_multiply_ip4key_ip4const', (SELECT count() FROM ip4 WHERE multiply(a, toIPv4('0.0.0.2')) > 6000000000) = (SELECT count() FROM oip4 WHERE multiply(a, toIPv4('0.0.0.2')) > 6000000000);
SELECT 'oracle_plus_ip4key_ip4const', (SELECT count() FROM ip4 WHERE plus(a, toIPv4('0.0.0.2')) > 3000000000) = (SELECT count() FROM oip4 WHERE plus(a, toIPv4('0.0.0.2')) > 3000000000);

SELECT '-- a genuine overflow must still reject monotonicity';
SELECT 'oracle_multiply_ip6key_overflow', (SELECT count() FROM ip6ovf WHERE multiply(a, 3) > 100) = (SELECT count() FROM oip6ovf WHERE multiply(a, 3) > 100);
SELECT 'noprune_multiply_ip6key_overflow_4of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip6ovf WHERE multiply(a, 3) > 100) WHERE explain ILIKE '%Granules: 4/4%';

SELECT '-- plus/minus over an IP key already pruned and must keep doing so';
SELECT 'prune_plus_ip4key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE plus(a, 10) > 3000000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_plus_ip4key', (SELECT count() FROM ip4 WHERE plus(a, 10) > 3000000000) = (SELECT count() FROM oip4 WHERE plus(a, 10) > 3000000000);
SELECT 'prune_minus_ip4key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE minus(a, 10) > 3000000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_minus_ip4key', (SELECT count() FROM ip4 WHERE minus(a, 10) > 3000000000) = (SELECT count() FROM oip4 WHERE minus(a, 10) > 3000000000);

SELECT '-- numeric constants stay unaffected';
SELECT 'oracle_multiply_zero', (SELECT count() FROM u32g4 WHERE multiply(a, 0) > 0) = (SELECT count() FROM ou32 WHERE multiply(a, 0) > 0);
SELECT 'oracle_multiply_negative', (SELECT count() FROM u32g4 WHERE multiply(CAST(a AS Int64), -3) > -70) = (SELECT count() FROM ou32 WHERE multiply(CAST(a AS Int64), -3) > -70);
SELECT 'oracle_multiply_decimal', (SELECT count() FROM u32g4 WHERE multiply(a, toDecimal32(2, 1)) > 40) = (SELECT count() FROM ou32 WHERE multiply(a, toDecimal32(2, 1)) > 40);
SELECT 'oracle_multiply_bool', (SELECT count() FROM u32g4 WHERE multiply(a, toBool(true)) > 20) = (SELECT count() FROM ou32 WHERE multiply(a, toBool(true)) > 20);
SELECT 'oracle_multiply_two_g1', (SELECT count() FROM u32g1 WHERE multiply(a, 2) > 40) = (SELECT count() FROM ou32 WHERE multiply(a, 2) > 40);

DROP TABLE u32g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE u32g4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ou32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ip4g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE oip4g1 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ip4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE oip4 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ctl32 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ip6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE oip6 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ip6ovf SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE oip6ovf SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ip4n SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE ip6be SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE oip6be SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE oip4n SETTINGS ignore_drop_queries_probability = 0;
