-- { echo }

-- `has` over a `Map` is not the same adapter as `mapContainsKey`. `FunctionArrayIndex::executeMap`
-- rewrites the map to an array of its keys and strips `LowCardinality` from both arguments before
-- comparing, so it compares the raw padded bytes, while `mapContainsKey` runs over the keys
-- subcolumn, which keeps the wrapper, and casts the constant to the dictionary type, stripping the
-- padding of a `FixedString`. A `bloom_filter` index on `mapKeys` must follow the former for `has`.
-- Cells compare the indexed answer against an unindexed oracle, so no expected value is baked in;
-- every reference row of that shape answers 1.

DROP TABLE IF EXISTS o_has_lc;
DROP TABLE IF EXISTS k_has_lc;
DROP TABLE IF EXISTS o_has_lcfs;
DROP TABLE IF EXISTS k_has_lcfs;

-- `LowCardinality(String)` keys.
CREATE TABLE o_has_lc (id UInt64, m Map(LowCardinality(String), UInt8)) ENGINE = Log;
CREATE TABLE k_has_lc (id UInt64, m Map(LowCardinality(String), UInt8),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_has_lc VALUES (0, {'K1':1}), (1, {'K1\0':2}), (2, {'X':3});
INSERT INTO k_has_lc VALUES (0, {'K1':1}), (1, {'K1\0':2}), (2, {'X':3});

SELECT (SELECT count() FROM k_has_lc WHERE has(m, toFixedString('K1', 3))) = (SELECT count() FROM o_has_lc WHERE has(m, toFixedString('K1', 3)));
SELECT (SELECT count() FROM k_has_lc WHERE has(m, toFixedString('K1', 2))) = (SELECT count() FROM o_has_lc WHERE has(m, toFixedString('K1', 2)));
SELECT (SELECT count() FROM k_has_lc WHERE has(m, 'K1')) = (SELECT count() FROM o_has_lc WHERE has(m, 'K1'));
SELECT (SELECT count() FROM k_has_lc WHERE has(m, 'X')) = (SELECT count() FROM o_has_lc WHERE has(m, 'X'));

-- `has` selects the physically padded key while `mapContainsKey` selects the unpadded one: pin both,
-- so the divergence cannot be lost to a shared coercion.
SELECT id FROM k_has_lc WHERE has(m, toFixedString('K1', 3)) ORDER BY id;
SELECT id FROM k_has_lc WHERE mapContainsKey(m, toFixedString('K1', 3)) ORDER BY id;

-- The index must still prune: more than zero and fewer than all granules are selected.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM k_has_lc WHERE has(m, toFixedString('K1', 3))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) > 0 AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

-- `LowCardinality(FixedString(3))` keys: `has` casts both sides to the least supertype, so an
-- over-wide constant still matches, while the dictionary cast of `mapContainsKey` rejects it by
-- width alone and the error must stay reachable.
CREATE TABLE o_has_lcfs (id UInt64, m Map(LowCardinality(FixedString(3)), UInt8)) ENGINE = Log;
CREATE TABLE k_has_lcfs (id UInt64, m Map(LowCardinality(FixedString(3)), UInt8),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_has_lcfs VALUES (0, {'K1':1}), (1, {'XYZ':2});
INSERT INTO k_has_lcfs VALUES (0, {'K1':1}), (1, {'XYZ':2});

SELECT (SELECT count() FROM k_has_lcfs WHERE has(m, 'K1')) = (SELECT count() FROM o_has_lcfs WHERE has(m, 'K1'));
SELECT (SELECT count() FROM k_has_lcfs WHERE has(m, toFixedString('K1', 2))) = (SELECT count() FROM o_has_lcfs WHERE has(m, toFixedString('K1', 2)));
SELECT (SELECT count() FROM k_has_lcfs WHERE has(m, toFixedString('K1', 3))) = (SELECT count() FROM o_has_lcfs WHERE has(m, toFixedString('K1', 3)));
SELECT (SELECT count() FROM k_has_lcfs WHERE has(m, toFixedString('K1', 5))) = (SELECT count() FROM o_has_lcfs WHERE has(m, toFixedString('K1', 5)));
SELECT id FROM k_has_lcfs WHERE has(m, toFixedString('K1', 5)) ORDER BY id;
SELECT count() FROM k_has_lcfs WHERE mapContainsKey(m, toFixedString('K1', 5)); -- { serverError TOO_LARGE_STRING_SIZE }

-- The supertype cast of `has` keeps the over-wide constant hashable, so the index is used instead
-- of declined.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM k_has_lcfs WHERE has(m, toFixedString('K1', 5))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) > 0 AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

DROP TABLE o_has_lc;
DROP TABLE k_has_lc;
DROP TABLE o_has_lcfs;
DROP TABLE k_has_lcfs;
