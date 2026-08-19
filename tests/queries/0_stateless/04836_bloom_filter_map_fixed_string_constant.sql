-- { echo }

-- `mapContainsKey`/`mapContainsValue`/`mapContains`, and `has` over a `Map`, are adapters of the
-- same array-search machinery as `has` over an array, so a `bloom_filter` index on `mapKeys`/
-- `mapValues` must coerce a string constant the same way. Cells compare the keyed answer against
-- an unindexed oracle, so no expected value is baked in; every reference row answers 1.

DROP TABLE IF EXISTS o_map;
DROP TABLE IF EXISTS k_map;
DROP TABLE IF EXISTS o_fs;
DROP TABLE IF EXISTS k_fs;
DROP TABLE IF EXISTS o_lc;
DROP TABLE IF EXISTS k_lc;
DROP TABLE IF EXISTS o_lcfs;
DROP TABLE IF EXISTS k_lcfs;

-- `String` keys, `FixedString(3)` values: keys compare the constant's raw padded bytes, values
-- coerce through the least supertype.
CREATE TABLE o_map (id UInt64, m Map(String, FixedString(3))) ENGINE = Log;
CREATE TABLE k_map (id UInt64, m Map(String, FixedString(3)),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1,
    INDEX iv mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_map VALUES (0, {'K1':'V0'}), (1, {'K1\0':'V0A'}), (2, {'X':'XYZ'});
INSERT INTO k_map VALUES (0, {'K1':'V0'}), (1, {'K1\0':'V0A'}), (2, {'X':'XYZ'});

SELECT (SELECT count() FROM k_map WHERE mapContainsValue(m, 'V0')) = (SELECT count() FROM o_map WHERE mapContainsValue(m, 'V0'));
SELECT (SELECT count() FROM k_map WHERE mapContainsValue(m, toFixedString('V0', 2))) = (SELECT count() FROM o_map WHERE mapContainsValue(m, toFixedString('V0', 2)));
SELECT (SELECT count() FROM k_map WHERE mapContainsValue(m, toFixedString('V0', 3))) = (SELECT count() FROM o_map WHERE mapContainsValue(m, toFixedString('V0', 3)));
SELECT (SELECT count() FROM k_map WHERE mapContainsValue(m, toFixedString('V0', 5))) = (SELECT count() FROM o_map WHERE mapContainsValue(m, toFixedString('V0', 5)));
SELECT (SELECT count() FROM k_map WHERE mapContainsKey(m, toFixedString('K1', 3))) = (SELECT count() FROM o_map WHERE mapContainsKey(m, toFixedString('K1', 3)));
SELECT (SELECT count() FROM k_map WHERE mapContainsKey(m, 'K1')) = (SELECT count() FROM o_map WHERE mapContainsKey(m, 'K1'));
SELECT (SELECT count() FROM k_map WHERE mapContains(m, 'K1')) = (SELECT count() FROM o_map WHERE mapContains(m, 'K1'));
SELECT (SELECT count() FROM k_map WHERE has(m, 'K1')) = (SELECT count() FROM o_map WHERE has(m, 'K1'));

-- The `String`-key raw-byte comparison matches the padded key only, and the index must not prune
-- the padded row: pin the selected rows.
SELECT id FROM k_map WHERE mapContainsKey(m, toFixedString('K1', 3)) ORDER BY id;
SELECT id FROM k_map WHERE mapContainsValue(m, toFixedString('V0', 5)) ORDER BY id;

-- `FixedString(3)` keys: both sides coerce through the least supertype, so an over-wide padded
-- constant still matches.
CREATE TABLE o_fs (id UInt64, m Map(FixedString(3), UInt8)) ENGINE = Log;
CREATE TABLE k_fs (id UInt64, m Map(FixedString(3), UInt8),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_fs VALUES (0, {'K1':1}), (1, {'K1A':2}), (2, {'XYZ':3});
INSERT INTO k_fs VALUES (0, {'K1':1}), (1, {'K1A':2}), (2, {'XYZ':3});

SELECT (SELECT count() FROM k_fs WHERE mapContainsKey(m, 'K1')) = (SELECT count() FROM o_fs WHERE mapContainsKey(m, 'K1'));
SELECT (SELECT count() FROM k_fs WHERE mapContainsKey(m, toFixedString('K1', 2))) = (SELECT count() FROM o_fs WHERE mapContainsKey(m, toFixedString('K1', 2)));
SELECT (SELECT count() FROM k_fs WHERE mapContainsKey(m, toFixedString('K1', 3))) = (SELECT count() FROM o_fs WHERE mapContainsKey(m, toFixedString('K1', 3)));
SELECT (SELECT count() FROM k_fs WHERE mapContainsKey(m, toFixedString('K1', 5))) = (SELECT count() FROM o_fs WHERE mapContainsKey(m, toFixedString('K1', 5)));

-- `LowCardinality(String)` keys: the constant casts straight to the dictionary type, which strips
-- the `FixedString` padding.
CREATE TABLE o_lc (id UInt64, m Map(LowCardinality(String), UInt8)) ENGINE = Log;
CREATE TABLE k_lc (id UInt64, m Map(LowCardinality(String), UInt8),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lc VALUES (0, {'K1':1}), (1, {'K1\0':2}), (2, {'X':3});
INSERT INTO k_lc VALUES (0, {'K1':1}), (1, {'K1\0':2}), (2, {'X':3});

SELECT (SELECT count() FROM k_lc WHERE mapContainsKey(m, toFixedString('K1', 2))) = (SELECT count() FROM o_lc WHERE mapContainsKey(m, toFixedString('K1', 2)));
SELECT (SELECT count() FROM k_lc WHERE mapContainsKey(m, toFixedString('K1', 3))) = (SELECT count() FROM o_lc WHERE mapContainsKey(m, toFixedString('K1', 3)));
SELECT id FROM k_lc WHERE mapContainsKey(m, toFixedString('K1', 3)) ORDER BY id;

-- `LowCardinality(FixedString(3))` keys: the direct cast to the dictionary type rejects an
-- over-wide constant by width alone, so the index must decline and keep the error reachable
-- instead of pruning everything into a silent 0.
CREATE TABLE o_lcfs (id UInt64, m Map(LowCardinality(FixedString(3)), UInt8)) ENGINE = Log;
CREATE TABLE k_lcfs (id UInt64, m Map(LowCardinality(FixedString(3)), UInt8),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lcfs VALUES (0, {'K1':1}), (1, {'XYZ':2});
INSERT INTO k_lcfs VALUES (0, {'K1':1}), (1, {'XYZ':2});

SELECT (SELECT count() FROM k_lcfs WHERE mapContainsKey(m, toFixedString('K1', 2))) = (SELECT count() FROM o_lcfs WHERE mapContainsKey(m, toFixedString('K1', 2)));
SELECT (SELECT count() FROM k_lcfs WHERE mapContainsKey(m, 'K1')) = (SELECT count() FROM o_lcfs WHERE mapContainsKey(m, 'K1'));
SELECT count() FROM o_lcfs WHERE mapContainsKey(m, toFixedString('K1', 5)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM k_lcfs WHERE mapContainsKey(m, toFixedString('K1', 5)); -- { serverError TOO_LARGE_STRING_SIZE }

-- Pruning is preserved where the index can hash exactly: some stage selects more than zero and
-- fewer than all granules.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM k_map WHERE mapContainsValue(m, toFixedString('V0', 5))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) > 0 AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM k_fs WHERE mapContainsKey(m, toFixedString('K1', 5))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) > 0 AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

DROP TABLE o_map;
DROP TABLE k_map;
DROP TABLE o_fs;
DROP TABLE k_fs;
DROP TABLE o_lc;
DROP TABLE k_lc;
DROP TABLE o_lcfs;
DROP TABLE k_lcfs;
