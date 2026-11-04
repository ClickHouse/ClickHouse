-- A `bloom_filter` index over `mapKeys` of a Map with `Enum` keys hashes the key as a value of the key
-- type, while the constant in the query is the name of the enum value, which is a `String`.
-- The key has to be converted to the key type before hashing, otherwise the index analysis throws.
-- The rewrite of `m['a']` to a key subcolumn is disabled here to reach the `arrayElement` path directly.

SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_map_enum_bf;

CREATE TABLE t_map_enum_bf
(
    id UInt64,
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), Int64),
    INDEX idx_keys mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_enum_bf VALUES (1, map('a', 10, 'c', 30)), (2, map('b', 20));

SELECT id FROM t_map_enum_bf WHERE m['a'] = 10 ORDER BY id;
SELECT id FROM t_map_enum_bf WHERE m['b'] IN (20) ORDER BY id;
SELECT id FROM t_map_enum_bf WHERE m['c'] != 30 ORDER BY id;

-- The same by the numeric value of the enum.
SELECT id FROM t_map_enum_bf WHERE m[1] = 10 ORDER BY id;
SELECT id FROM t_map_enum_bf WHERE m[3] IN (30) ORDER BY id;

-- A key that is not in the Enum at all: `arrayElement` returns the default value, and the index
-- cannot be used, but nothing throws.
SELECT id FROM t_map_enum_bf WHERE m[toInt8(4)] = 10 ORDER BY id;
SELECT id FROM t_map_enum_bf WHERE m['nonexistent'] = 10 ORDER BY id; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

-- The index is still used and prunes the granules that do not have the key.
SELECT count() FROM t_map_enum_bf WHERE m['a'] = 10;
SELECT count() FROM t_map_enum_bf WHERE m['b'] = 10;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_enum_bf WHERE m['b'] = 20) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

-- The same with the subcolumn rewrite enabled.
SET optimize_functions_to_subcolumns = 1;
SELECT id FROM t_map_enum_bf WHERE m['a'] = 10 ORDER BY id;
SELECT id FROM t_map_enum_bf WHERE m[1] = 10 ORDER BY id;

DROP TABLE t_map_enum_bf;

-- With both a `mapKeys` and a `mapValues` index, a key that is not in the `Enum` must not be probed
-- by the values index alone: that could prune the granules before `arrayElement` runs and silently
-- replace the exception by an empty result.
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_map_enum_bf_keys_values;

CREATE TABLE t_map_enum_bf_keys_values
(
    id UInt64,
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), Int64),
    INDEX idx_keys mapKeys(m) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_values mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_enum_bf_keys_values VALUES (1, map('a', 10, 'c', 30)), (2, map('b', 20));

SELECT id FROM t_map_enum_bf_keys_values WHERE m['nonexistent'] = 999 ORDER BY id; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT id FROM t_map_enum_bf_keys_values WHERE m['a'] = 10 ORDER BY id;
SELECT id FROM t_map_enum_bf_keys_values WHERE m['b'] = 20 ORDER BY id;

DROP TABLE t_map_enum_bf_keys_values;

-- With only a `mapValues` index, the key type is not available from the index header, so it is taken
-- from the type of the map column itself. A key that is not in the `Enum` must decline the whole
-- predicate for the same reason as above.
DROP TABLE IF EXISTS t_map_enum_bf_values;

CREATE TABLE t_map_enum_bf_values
(
    id UInt64,
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), Int64),
    INDEX idx_values mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_enum_bf_values VALUES (1, map('a', 10, 'c', 30)), (2, map('b', 20));

SELECT id FROM t_map_enum_bf_values WHERE m['nonexistent'] = 999 ORDER BY id; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT id FROM t_map_enum_bf_values WHERE m['a'] = 10 ORDER BY id;
SELECT id FROM t_map_enum_bf_values WHERE m['b'] = 20 ORDER BY id;

DROP TABLE t_map_enum_bf_values;

-- A `mapValues` index over `Enum` values: the constant can be the name of the enum value, a `String`.
-- It has to be converted to the value of the enum before it is hashed. For a missing key `arrayElement`
-- returns the zero of the underlying integer, which matches the name of the enum value 0, so the index
-- cannot be used for it.
DROP TABLE IF EXISTS t_map_enum_values_bf;

CREATE TABLE t_map_enum_values_bf
(
    id UInt64,
    m Map(String, Enum8('z' = 0, 'a' = 1, 'b' = 2, 'c' = 3)),
    INDEX idx_values mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_enum_values_bf VALUES (1, map('k', 'b')), (2, map('k', 'c')), (3, map('x', 'b'));

SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 'b' ORDER BY id;
SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 'z' ORDER BY id;
SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 'a' ORDER BY id;
SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 1 ORDER BY id;

-- The same without the index: the results must agree.
SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 'b' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 'z' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 'a' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 1 ORDER BY id SETTINGS use_skip_indexes = 0;

-- The index is used and prunes the granules that do not have the value.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_enum_values_bf WHERE m['k'] = 'c') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE t_map_enum_values_bf;
