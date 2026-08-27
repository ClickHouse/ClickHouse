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
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_enum_bf WHERE m['b'] = 20) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

-- The same with the subcolumn rewrite enabled.
SET optimize_functions_to_subcolumns = 1;
SELECT id FROM t_map_enum_bf WHERE m['a'] = 10 ORDER BY id;
SELECT id FROM t_map_enum_bf WHERE m[1] = 10 ORDER BY id;

DROP TABLE t_map_enum_bf;
