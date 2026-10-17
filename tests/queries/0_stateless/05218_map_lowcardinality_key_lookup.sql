-- m[key] on a Map with LowCardinality keys is resolved through dictionary positions instead of
-- comparing the key values. The result must be the same as with the generic comparison, and the
-- same for both the arrayElement path (optimize_functions_to_subcolumns = 0) and the subcolumn
-- path (optimize_functions_to_subcolumns = 1).

DROP TABLE IF EXISTS t_map_lc;

CREATE TABLE t_map_lc (id UInt64, m Map(LowCardinality(String), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

-- Row 2 has no 'b' at all, row 3 is an empty map, row 4 has 'b' twice and '' as a key.
INSERT INTO t_map_lc VALUES (1, map('a', 'a1', 'b', 'b1')), (2, map('a', 'a2')), (3, map()), (4, map('', 'empty', 'b', 'FIRST', 'b', 'SECOND'));

SELECT 'present key, subcolumns=0', id, m['b'] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'present key, subcolumns=1', id, m['b'] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

-- A key that is in no row is absent from the dictionary as well, and is resolved without
-- looking at the rows.
SELECT 'absent key, subcolumns=0', id, m['zz'] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'absent key, subcolumns=1', id, m['zz'] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

-- The empty string is kept in the reserved default position of the dictionary, not in its index.
SELECT 'empty key, subcolumns=0', id, m[''] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'empty key, subcolumns=1', id, m[''] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

-- Reading the subcolumn directly goes through the same key lookup.
SELECT 'direct subcolumn', id, m.key_b FROM t_map_lc ORDER BY id;

DROP TABLE t_map_lc;

-- Compact parts exercise the other reader path.
CREATE TABLE t_map_lc (id UInt64, m Map(LowCardinality(String), String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_map_lc VALUES (1, map('a', 'a1', 'b', 'b1')), (2, map('a', 'a2')), (3, map()), (4, map('', 'empty', 'b', 'FIRST', 'b', 'SECOND'));

SELECT 'compact, subcolumns=0', id, m['b'] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'compact, subcolumns=1', id, m['b'] FROM t_map_lc ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_lc;

-- LowCardinality(FixedString) keys use the same dictionary lookup.
CREATE TABLE t_map_lc_fixed (id UInt64, m Map(LowCardinality(FixedString(3)), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_lc_fixed VALUES (1, map('aaa', 'a1', 'bbb', 'b1')), (2, map('aaa', 'a2'));

SELECT 'fixed string key, subcolumns=0', id, m['bbb'], m['zzz'] FROM t_map_lc_fixed ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'fixed string key, subcolumns=1', id, m['bbb'], m['zzz'] FROM t_map_lc_fixed ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_lc_fixed;

-- A wide map, where the looked up key is the last one in every row: the shape the dictionary
-- lookup is meant to make cheap.
DROP TABLE IF EXISTS t_map_lc_wide;

CREATE TABLE t_map_lc_wide (id UInt64, m Map(LowCardinality(String), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_map_lc_wide
SELECT number, mapFromArrays(arrayMap(x -> concat('k', leftPad(toString(x), 2, '0')), range(20)), arrayMap(x -> concat('v', toString(number), '_', toString(x)), range(20)))
FROM numbers(1000);

SELECT 'wide map, subcolumns=0', sum(cityHash64(m['k19'])), sum(cityHash64(m['k00'])) FROM t_map_lc_wide SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'wide map, subcolumns=1', sum(cityHash64(m['k19'])), sum(cityHash64(m['k00'])) FROM t_map_lc_wide SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_lc_wide;

-- A key at a high dictionary position looked up over rows that do not contain it. The dictionary of
-- a part is shared by all of its blocks, so a dictionary position can be out of the range of the
-- index type of an individual block, and must not be truncated into a false match.
DROP TABLE IF EXISTS t_map_lc_many_keys;

CREATE TABLE t_map_lc_many_keys (id UInt64, m Map(LowCardinality(String), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_map_lc_many_keys
SELECT number, map(concat('k', leftPad(toString(number % 1000), 4, '0')), concat('v', toString(number)))
FROM numbers(100000);

SELECT 'high dictionary position, subcolumns=0', count(), countIf(m['k0999'] != '') FROM t_map_lc_many_keys SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'high dictionary position, subcolumns=1', count(), countIf(m['k0999'] != '') FROM t_map_lc_many_keys SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_lc_many_keys;

-- Pseudo-random wide maps with duplicate keys: both paths must agree, which is the property that
-- the first-match fix of issue #111203 established.
DROP TABLE IF EXISTS t_map_lc_random;

CREATE TABLE t_map_lc_random (id UInt64, m Map(LowCardinality(String), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_map_lc_random
SELECT
    number,
    mapFromArrays(
        arrayMap(x -> concat('k', toString(cityHash64(number, x) % 12)), range(1 + (number % 15))),
        arrayMap(x -> concat('v', toString(cityHash64(number, x, 'value'))), range(1 + (number % 15))))
FROM numbers(20000);

SELECT 'random maps, subcolumns=0', sum(cityHash64(m['k0'], m['k5'], m['k11'], m['k99'])) FROM t_map_lc_random SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'random maps, subcolumns=1', sum(cityHash64(m['k0'], m['k5'], m['k11'], m['k99'])) FROM t_map_lc_random SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_lc_random;

-- Key types that used to be compared through virtual compareAt.

DROP TABLE IF EXISTS t_map_key_types;

CREATE TABLE t_map_key_types (id UInt64, m Map(UUID, String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_key_types VALUES (1, map('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'hit')), (2, map('00000000-0000-0000-0000-000000000001', 'other'));
SELECT 'UUID key, subcolumns=0', id, m[toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'UUID key, subcolumns=1', id, m[toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_key_types;

CREATE TABLE t_map_key_types (id UInt64, m Map(IPv4, String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_key_types VALUES (1, map('1.2.3.4', 'hit')), (2, map('5.6.7.8', 'other'));
SELECT 'IPv4 key, subcolumns=0', id, m[toIPv4('1.2.3.4')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'IPv4 key, subcolumns=1', id, m[toIPv4('1.2.3.4')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_key_types;

CREATE TABLE t_map_key_types (id UInt64, m Map(IPv6, String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_key_types VALUES (1, map('::1', 'hit')), (2, map('::2', 'other'));
SELECT 'IPv6 key, subcolumns=0', id, m[toIPv6('::1')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'IPv6 key, subcolumns=1', id, m[toIPv6('::1')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_key_types;

-- The wide integers are built from strings, because a literal that wide goes through Float64 and
-- loses precision.
CREATE TABLE t_map_key_types (id UInt64, m Map(Int128, String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_key_types SELECT 1, map(toInt128('-123456789012345678901234567890'), 'hit');
INSERT INTO t_map_key_types SELECT 2, map(toInt128(1), 'other');
SELECT 'Int128 key, subcolumns=0', id, m[toInt128('-123456789012345678901234567890')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'Int128 key, subcolumns=1', id, m[toInt128('-123456789012345678901234567890')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_key_types;

CREATE TABLE t_map_key_types (id UInt64, m Map(UInt256, String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_key_types SELECT 1, map(toUInt256('12345678901234567890123456789012345678901234567890'), 'hit');
INSERT INTO t_map_key_types SELECT 2, map(toUInt256(1), 'other');
SELECT 'UInt256 key, subcolumns=0', id, m[toUInt256('12345678901234567890123456789012345678901234567890')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'UInt256 key, subcolumns=1', id, m[toUInt256('12345678901234567890123456789012345678901234567890')] FROM t_map_key_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_key_types;

-- Decimal and DateTime64 keys are rejected by m[key], but reachable through the subcolumn name.

CREATE TABLE t_map_key_types (id UInt64, m Map(Decimal64(2), String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_key_types SELECT 1, map(toDecimal64(1.5, 2), 'hit', toDecimal64(2.5, 2), 'other');
INSERT INTO t_map_key_types SELECT 2, map(toDecimal64(2.5, 2), 'other');
SELECT 'Decimal64 key subcolumn', id, `m.key_1.50` FROM t_map_key_types ORDER BY id;
DROP TABLE t_map_key_types;

-- The timezone is pinned, because the key is rendered into the subcolumn name.
CREATE TABLE t_map_key_types (id UInt64, m Map(DateTime64(3, 'UTC'), String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_key_types SELECT 1, map(toDateTime64('2020-01-01 00:00:00.500', 3, 'UTC'), 'hit', toDateTime64('2021-01-01 00:00:00.500', 3, 'UTC'), 'other');
INSERT INTO t_map_key_types SELECT 2, map(toDateTime64('2021-01-01 00:00:00.500', 3, 'UTC'), 'other');
SELECT 'DateTime64 key subcolumn', id, `m.key_2020-01-01 00:00:00.500` FROM t_map_key_types ORDER BY id;
DROP TABLE t_map_key_types;

-- Value types that used to be copied through virtual insertFrom. The rows not holding the key
-- cover the default-insert branch, the Nullable variants the null map.

DROP TABLE IF EXISTS t_map_value_types;

CREATE TABLE t_map_value_types (id UInt64, m Map(String, UUID)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_value_types VALUES (1, map('a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0')), (2, map('b', '00000000-0000-0000-0000-000000000001'));
SELECT 'UUID value, subcolumns=0', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'UUID value, subcolumns=1', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_value_types;

CREATE TABLE t_map_value_types (id UInt64, m Map(String, IPv6)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_value_types VALUES (1, map('a', '::1')), (2, map('b', '::2'));
SELECT 'IPv6 value, subcolumns=0', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'IPv6 value, subcolumns=1', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_value_types;

CREATE TABLE t_map_value_types (id UInt64, m Map(String, Int128)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_value_types SELECT 1, map('a', toInt128('-123456789012345678901234567890'));
INSERT INTO t_map_value_types SELECT 2, map('b', toInt128(1));
SELECT 'Int128 value, subcolumns=0', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'Int128 value, subcolumns=1', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_value_types;

CREATE TABLE t_map_value_types (id UInt64, m Map(String, Decimal64(3))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_value_types VALUES (1, map('a', 1.25)), (2, map('b', 2.5));
SELECT 'Decimal64 value, subcolumns=0', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'Decimal64 value, subcolumns=1', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_value_types;

-- The timezone is pinned, because the missing key renders the epoch as the default value.
CREATE TABLE t_map_value_types (id UInt64, m Map(String, DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_value_types VALUES (1, map('a', '2020-01-01 00:00:00.500')), (2, map('b', '2021-01-01 00:00:00.500'));
SELECT 'DateTime64 value, subcolumns=0', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'DateTime64 value, subcolumns=1', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_value_types;

CREATE TABLE t_map_value_types (id UInt64, m Map(String, Nullable(UUID))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_value_types VALUES (1, map('a', '61f0c404-5cb3-11e7-907b-a6006ad3dba0')), (2, map('a', NULL)), (3, map('b', NULL));
SELECT 'Nullable(UUID) value, subcolumns=0', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'Nullable(UUID) value, subcolumns=1', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_value_types;

CREATE TABLE t_map_value_types (id UInt64, m Map(String, Nullable(Decimal64(3)))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_value_types VALUES (1, map('a', 1.25)), (2, map('a', NULL)), (3, map('b', NULL));
SELECT 'Nullable(Decimal64) value, subcolumns=0', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'Nullable(Decimal64) value, subcolumns=1', id, m['a'] FROM t_map_value_types ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_value_types;
