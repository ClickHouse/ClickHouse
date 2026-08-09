-- Tags: no-parallel-replicas

-- `plus`/`minus`/`multiply` substitute an `IPv4`/`IPv6` operand with `UInt32`/`UInt128`, so key
-- analysis must not compare the raw IP-tagged `Field` against a number. Every `oracle_` row compares
-- a keyed `MergeTree` against an `ENGINE = Log` oracle; a `1` means key analysis agrees with
-- execution. `prune_` rows assert the granule counts, so a change that quietly degrades the
-- optimization also fails.
--
-- Coverage invariant: every IP-carrying arithmetic shape asserted here has EITHER a granule row
-- (`prune_`/`noprune_`) OR a single-granule fixture that makes such a row vacuous. A result-only
-- `oracle_` row cannot see a fix that avoids the exception by refusing to report monotonicity, so
-- the granule rows below are what separate this fix from that weaker one. Every granule literal is
-- measured, and each IP shape is paired with a numeric-constant control at the same threshold and
-- table so a count that merely reflects predicate selectivity cannot pass for preserved pruning.

DROP TABLE IF EXISTS u32g1;
DROP TABLE IF EXISTS u32g4;
DROP TABLE IF EXISTS ou32;
DROP TABLE IF EXISTS ip4g1;
DROP TABLE IF EXISTS oip4g1;
DROP TABLE IF EXISTS ip4;
DROP TABLE IF EXISTS oip4;
DROP TABLE IF EXISTS ctl32;
DROP TABLE IF EXISTS ip6;
DROP TABLE IF EXISTS oip6;
DROP TABLE IF EXISTS ip6ovf;
DROP TABLE IF EXISTS oip6ovf;
DROP TABLE IF EXISTS ip4n;
DROP TABLE IF EXISTS ip6be;
DROP TABLE IF EXISTS oip6be;
DROP TABLE IF EXISTS oip4n;
DROP TABLE IF EXISTS u32g2;

-- Numeric key, IP constant. Granularity 1 makes every granule a min == max interval, which is the
-- only way `plus`/`minus` reach the equal-endpoints fast path.
CREATE TABLE u32g1 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE u32g4 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
CREATE TABLE ou32 (a UInt32) ENGINE = Log;
INSERT INTO u32g1 VALUES (10), (20), (30), (40);
INSERT INTO u32g4 VALUES (10), (20), (30), (40);
INSERT INTO ou32 VALUES (10), (20), (30), (40);

-- A second, non-vacuous granularity for the numeric-key control. `u32g4` holds four rows at
-- granularity 4, i.e. a single granule, so a granule assertion there could only ever read `1/1`;
-- eight rows at granularity 2 give four real granules instead.
CREATE TABLE u32g2 (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2;
INSERT INTO u32g2 VALUES (10), (20), (30), (40), (50), (60), (70), (80);

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
-- endpoint, pinned by the `prune_*_const_left_ip4key_2of4` rows below, and the constant, pinned here
-- (a right-role constant short-circuits the endpoint comparison away). Deferring the
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

-- Granule assertions for every remaining IP-carrying shape that had only an `oracle_` row. These
-- are the rows a numeric type gate cannot survive: gating on the constant's declared type avoids
-- the exception without normalizing the value, which leaves every `oracle_` row green and silently
-- drops pruning. `multiply` is the discriminating function here because its arm is the one that
-- reads the constant AND both endpoints; `plus`/`minus` reach the comparison only through the
-- equal-endpoints fast path, so their counts are unchanged by such a gate and their rows below are
-- must-not-regress controls rather than discriminators.
SELECT '-- every IP-carrying arithmetic shape keeps its measured granule count';
-- multiply x IPv4 constant x numeric key, BOTH operand roles: `const_side` picks whichever side is
-- constant, so the role is a real axis. `< 45` is required: at `< 40` only one granule survives and
-- at `>= 145` all four do, so neither threshold separates the fix from the gate.
SELECT 'prune_multiply_ip4const_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(a, toIPv4('0.0.0.2')) < 45) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_ip4const_left_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(toIPv4('0.0.0.2'), a) < 45) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_numconst_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(a, toUInt32(2)) < 45) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_numconst_left_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(toUInt32(2), a) < 45) WHERE explain ILIKE '%Granules: 2/4%';
-- multiply x IPv6 constant x numeric key, both roles: the byte-order path.
SELECT 'oracle_multiply_ip6const_right_g1', (SELECT count() FROM u32g1 WHERE multiply(a, toIPv6('::2')) < 45) = (SELECT count() FROM ou32 WHERE multiply(a, toIPv6('::2')) < 45);
SELECT 'prune_multiply_ip6const_right_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(a, toIPv6('::2')) < 45) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'oracle_multiply_ip6const_left_g1', (SELECT count() FROM u32g1 WHERE multiply(toIPv6('::2'), a) < 45) = (SELECT count() FROM ou32 WHERE multiply(toIPv6('::2'), a) < 45);
SELECT 'prune_multiply_ip6const_left_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(toIPv6('::2'), a) < 45) WHERE explain ILIKE '%Granules: 2/4%';
-- multiply x numeric constant x numeric key at the second, non-vacuous granularity: the
-- must-not-regress control proving the normalization did not perturb the plain numeric path.
SELECT 'prune_multiply_ip4const_right_g2_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g2 WHERE multiply(a, toIPv4('0.0.0.2')) < 65) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_numconst_right_g2_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g2 WHERE multiply(a, toUInt32(2)) < 65) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_numconst_left_g2_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g2 WHERE multiply(toUInt32(2), a) < 65) WHERE explain ILIKE '%Granules: 2/4%';
-- minus x IPv4 constant, left role. The right role is already pinned above; this is the mirror.
SELECT 'prune_minus_ip4const_left_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE minus(toIPv4('0.0.0.2'), a) > -25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_minus_numconst_left_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE minus(toUInt32(2), a) > -25) WHERE explain ILIKE '%Granules: 2/4%';
-- Two IP operands at once, granule half: the constant and both endpoints are IP-tagged in the same
-- call, so both normalization statements fire together. `multiply` here is the shape Gate B named.
SELECT 'prune_multiply_ip4key_ip4const_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE multiply(a, toIPv4('0.0.0.2')) > 6000000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_plus_ip4key_ip4const_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE plus(a, toIPv4('0.0.0.2')) > 3000000000) WHERE explain ILIKE '%Granules: 2/4%';
-- Wrapper constants over a numeric key: `Nullable` and `LowCardinality` must reach the same
-- normalization as the bare IP constant, so they get the granule row their oracles lacked.
SELECT 'prune_plus_nullable_ip4const_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE plus(a, CAST(toIPv4('0.0.0.2') AS Nullable(IPv4))) < 25) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_plus_lc_ip4const_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE plus(a, toLowCardinality(toIPv4('0.0.0.2'))) < 25) WHERE explain ILIKE '%Granules: 2/4%';
-- The same wrapper matrix for `multiply`, whose arm normalizes the constant as well as both endpoints,
-- so a gate keyed on the constant's IP tag is visible here and in none of the `plus` rows above.
-- `prune_multiply_numconst_right_g1_2of4` is the control: same table, same threshold.
SELECT 'prune_multiply_nullable_ip4const_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(a, CAST(toIPv4('0.0.0.2') AS Nullable(IPv4))) < 45) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_lc_ip4const_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(a, toLowCardinality(toIPv4('0.0.0.2'))) < 45) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_nullable_ip6const_g1_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u32g1 WHERE multiply(a, CAST(toIPv6('::2') AS Nullable(IPv6))) < 45) WHERE explain ILIKE '%Granules: 2/4%';
-- A numeric constant in the LEFT role over an IP key. `right_arg_is_zero` short-circuits on the LEFT
-- operand being the constant and then compares a KEY endpoint, so these three shapes are the only ones
-- reaching the IP-vs-zero comparison with an endpoint as its operand; every right-role row above
-- leaves that conjunct unevaluated. `ctl32` holds the identical bit values, so it controls the count.
SELECT 'prune_plus_const_left_ip4key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE plus(1, a) < 40000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_plus_const_left_u32key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctl32 WHERE plus(1, a) < 40000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_minus_const_left_ip4key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE minus(1, a) > -40000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_minus_const_left_u32key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctl32 WHERE minus(1, a) > -40000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_const_left_ip4key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ip4 WHERE multiply(1, a) < 40000000) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'prune_multiply_const_left_u32key_2of4', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctl32 WHERE multiply(1, a) < 40000000) WHERE explain ILIKE '%Granules: 2/4%';

SELECT '-- numeric constants stay unaffected';
SELECT 'oracle_multiply_zero', (SELECT count() FROM u32g4 WHERE multiply(a, 0) > 0) = (SELECT count() FROM ou32 WHERE multiply(a, 0) > 0);
SELECT 'oracle_multiply_negative', (SELECT count() FROM u32g4 WHERE multiply(CAST(a AS Int64), -3) > -70) = (SELECT count() FROM ou32 WHERE multiply(CAST(a AS Int64), -3) > -70);
SELECT 'oracle_multiply_decimal', (SELECT count() FROM u32g4 WHERE multiply(a, toDecimal32(2, 1)) > 40) = (SELECT count() FROM ou32 WHERE multiply(a, toDecimal32(2, 1)) > 40);
SELECT 'oracle_multiply_bool', (SELECT count() FROM u32g4 WHERE multiply(a, toBool(true)) > 20) = (SELECT count() FROM ou32 WHERE multiply(a, toBool(true)) > 20);
SELECT 'oracle_multiply_two_g1', (SELECT count() FROM u32g1 WHERE multiply(a, 2) > 40) = (SELECT count() FROM ou32 WHERE multiply(a, 2) > 40);

DROP TABLE u32g1;
DROP TABLE u32g4;
DROP TABLE ou32;
DROP TABLE ip4g1;
DROP TABLE oip4g1;
DROP TABLE ip4;
DROP TABLE oip4;
DROP TABLE ctl32;
DROP TABLE ip6;
DROP TABLE oip6;
DROP TABLE ip6ovf;
DROP TABLE oip6ovf;
DROP TABLE ip4n;
DROP TABLE ip6be;
DROP TABLE oip6be;
DROP TABLE oip4n;
DROP TABLE u32g2;
