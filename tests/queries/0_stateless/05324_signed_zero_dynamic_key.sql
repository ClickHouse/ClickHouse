-- `-0.0 = 0.0` holds for a float stored in `Dynamic` too, so a key over `d::String` (where the zeros are
-- '-0' and '0') must not skip the granule holding the other zero. Each query compares the result with the
-- primary key against the result without it.
-- https://github.com/ClickHouse/ClickHouse/issues/123743

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_signed_zero_dynamic;
CREATE TABLE t_signed_zero_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic VALUES (-0.0), (-0.5), (0.0), (2.0);

SELECT 'd = 0.0', count(), (SELECT count() FROM t_signed_zero_dynamic WHERE d = 0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic WHERE d = 0.0;
SELECT 'd = -0.0', count(), (SELECT count() FROM t_signed_zero_dynamic WHERE d = -0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic WHERE d = -0.0;
SELECT 'd = 0', count(), (SELECT count() FROM t_signed_zero_dynamic WHERE d = 0 SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic WHERE d = 0;
-- The comparison converts the string to the float, so it matches both zeros as well.
SELECT 'd = \'0\'', count(), (SELECT count() FROM t_signed_zero_dynamic WHERE d = '0' SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic WHERE d = '0';
SELECT 'd = 2.0', count(), (SELECT count() FROM t_signed_zero_dynamic WHERE d = 2.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic WHERE d = 2.0;

DROP TABLE t_signed_zero_dynamic;

-- `Dynamic` inside a `Tuple`.
DROP TABLE IF EXISTS t_signed_zero_dynamic_tuple;
CREATE TABLE t_signed_zero_dynamic_tuple (k Tuple(Dynamic, UInt8)) ENGINE = MergeTree ORDER BY toString(k) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_tuple VALUES ((-0.0, 1)), ((0.5, 1)), ((0.0, 1)), ((2.0, 1));
SELECT 'k = (0.0, 1)', count(), (SELECT count() FROM t_signed_zero_dynamic_tuple WHERE k = (0.0, 1) SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic_tuple WHERE k = (0.0, 1);
DROP TABLE t_signed_zero_dynamic_tuple;

-- The comparison parses a string into the type of each stored variant, so a string that is not a zero as a
-- `Float64` can still be one as a narrower float: `'1e-50'` underflows to zero as a `Float32`, and `'1e-42'`
-- as a `BFloat16`.
DROP TABLE IF EXISTS t_signed_zero_dynamic_float32;
DROP TABLE IF EXISTS t_signed_zero_dynamic_bfloat16;
CREATE TABLE t_signed_zero_dynamic_float32 (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_float32 VALUES (-0.0::Float32), (0.0::Float32), (2::Float32);
CREATE TABLE t_signed_zero_dynamic_bfloat16 (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_bfloat16 VALUES (-0.0::BFloat16), (0.0::BFloat16), (2::BFloat16);

SELECT 'Float32: d = \'1e-50\'', count(), (SELECT count() FROM t_signed_zero_dynamic_float32 WHERE d = '1e-50' SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic_float32 WHERE d = '1e-50';
SELECT 'BFloat16: d = \'1e-42\'', count(), (SELECT count() FROM t_signed_zero_dynamic_bfloat16 WHERE d = '1e-42' SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic_bfloat16 WHERE d = '1e-42';

DROP TABLE t_signed_zero_dynamic_float32;
DROP TABLE t_signed_zero_dynamic_bfloat16;

-- A number spelled in a non-canonical way: the stored float is keyed as '42', but the comparison parses '042' to 42.
DROP TABLE IF EXISTS t_signed_zero_dynamic_non_canonical;
CREATE TABLE t_signed_zero_dynamic_non_canonical (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_non_canonical SELECT number::Float64 FROM numbers(100);
SELECT 'd = \'042\'', count(), (SELECT count() FROM t_signed_zero_dynamic_non_canonical WHERE d = '042' SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic_non_canonical WHERE d = '042';
DROP TABLE t_signed_zero_dynamic_non_canonical;

-- Only a constant that can be a zero gives up the key: a non-zero number, or a string that does not parse as
-- a number, still selects a few granules out of 100.
DROP TABLE IF EXISTS t_signed_zero_dynamic_strings;
DROP TABLE IF EXISTS t_signed_zero_dynamic_floats;
CREATE TABLE t_signed_zero_dynamic_strings (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_strings SELECT concat('s', toString(number)) FROM numbers(100);
CREATE TABLE t_signed_zero_dynamic_floats (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_floats SELECT number::Float64 FROM numbers(100);

SELECT 'pruned for \'s42\'', count() < 10 FROM t_signed_zero_dynamic_strings WHERE d = 's42' SETTINGS force_primary_key = 1, max_rows_to_read = 10;
SELECT 'pruned for 42.0', count() < 10 FROM t_signed_zero_dynamic_floats WHERE d = 42.0 SETTINGS force_primary_key = 1, max_rows_to_read = 10;

DROP TABLE t_signed_zero_dynamic_strings;
DROP TABLE t_signed_zero_dynamic_floats;

-- A `UUID`, `IPv4` or `IPv6` constant is not a float, so it keeps using the key.
DROP TABLE IF EXISTS t_signed_zero_dynamic_uuid;
DROP TABLE IF EXISTS t_signed_zero_dynamic_ipv4;
DROP TABLE IF EXISTS t_signed_zero_dynamic_ipv6;
CREATE TABLE t_signed_zero_dynamic_uuid (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_uuid SELECT toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0'))) FROM numbers(100);
CREATE TABLE t_signed_zero_dynamic_ipv4 (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_ipv4 SELECT toIPv4(number) FROM numbers(100);
CREATE TABLE t_signed_zero_dynamic_ipv6 (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_ipv6 SELECT toIPv6(toIPv4(number)) FROM numbers(100);

SELECT 'pruned for a UUID', count() FROM t_signed_zero_dynamic_uuid WHERE d = toUUID('00000000-0000-0000-0000-000000000042') SETTINGS force_primary_key = 1, max_rows_to_read = 10;
SELECT 'pruned for an IPv4', count() FROM t_signed_zero_dynamic_ipv4 WHERE d = toIPv4('0.0.0.42') SETTINGS force_primary_key = 1, max_rows_to_read = 10;
SELECT 'pruned for an IPv6', count() FROM t_signed_zero_dynamic_ipv6 WHERE d = toIPv6('::ffff:0.0.0.42') SETTINGS force_primary_key = 1, max_rows_to_read = 10;

DROP TABLE t_signed_zero_dynamic_uuid;
DROP TABLE t_signed_zero_dynamic_ipv4;
DROP TABLE t_signed_zero_dynamic_ipv6;

-- A `Dynamic` value can be an array of floats, so an array constant holding a zero must not skip the other zero.
DROP TABLE IF EXISTS t_signed_zero_dynamic_array;
CREATE TABLE t_signed_zero_dynamic_array (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_dynamic_array VALUES ([-0.0]), ([0.5]), ([0.0]);
SELECT 'd = [0.0]', count(), (SELECT count() FROM t_signed_zero_dynamic_array WHERE d = [0.0] SETTINGS use_primary_key = 0) FROM t_signed_zero_dynamic_array WHERE d = [0.0];
DROP TABLE t_signed_zero_dynamic_array;
