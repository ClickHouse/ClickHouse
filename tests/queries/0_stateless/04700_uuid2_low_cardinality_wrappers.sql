-- Tags: shard

-- Regression test for `LowCardinality` wrappers suppressing the `UUID` / `UUID2` layout handling:
-- a `LowCardinality`-wrapped lookup key, needle, array or constant must behave exactly like its
-- full-column counterpart. The two types share the physical representation but keep the two 64-bit
-- halves in the opposite order, so a missed layout cast silently matches nothing.
-- The UUIDs below are random on purpose: a value symmetric under a half swap would hide the bug.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SET allow_suspicious_low_cardinality_types = 1;

-- 1. Map subscript: `LowCardinality`-wrapped lookup key of the other UUID flavor.
DROP TABLE IF EXISTS map_uuid2;
CREATE TABLE map_uuid2 (m Map(UUID2, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO map_uuid2 VALUES (map(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 42));
SELECT 'map_subscript_lc_key', m[toLowCardinality(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))] FROM map_uuid2;
SELECT 'map_subscript_lc_key_subcolumns', m[toLowCardinality(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))] FROM map_uuid2
SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE map_uuid2;

-- 2. Map with `LowCardinality` keys: a plain lookup key of the other UUID flavor.
DROP TABLE IF EXISTS map_lc_uuid2;
CREATE TABLE map_lc_uuid2 (m Map(LowCardinality(UUID2), UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO map_lc_uuid2 VALUES (map(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 42));
SELECT 'map_lc_keys_plain_key', m[toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')] FROM map_lc_uuid2;
DROP TABLE map_lc_uuid2;

-- 3. `has` / `indexOf`: `LowCardinality`-wrapped needle of the other UUID flavor.
SELECT 'has_lc_needle', has([toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toLowCardinality(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));
SELECT 'indexOf_lc_needle', indexOf([toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toLowCardinality(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));

-- 4. `has` / `indexOf`: `Array(LowCardinality(UUID2))` haystack with a plain `UUID` needle.
SELECT 'has_lc_array', has(CAST(['61f0c404-5cb3-11e7-907b-a6006ad3dba0'], 'Array(LowCardinality(UUID2))'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT 'indexOf_lc_array', indexOf(CAST(['61f0c404-5cb3-11e7-907b-a6006ad3dba0'], 'Array(LowCardinality(UUID2))'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));

-- 5. Distributed query: a `LowCardinality(UUID2)` constant must reach the shard with its canonical
--    textual value, not the raw half-swapped `UUID`-layout field from the dictionary.
DROP TABLE IF EXISTS dist_uuid2;
CREATE TABLE dist_uuid2 (id UUID2) ENGINE = MergeTree ORDER BY id;
INSERT INTO dist_uuid2 VALUES (toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT 'remote_lc_constant', count() FROM remote('127.2', currentDatabase(), dist_uuid2)
WHERE id = toLowCardinality(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
DROP TABLE dist_uuid2;
