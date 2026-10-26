-- For a missing key `arrayElement` over `Enum` map values returns the zero of the underlying integer,
-- which is not the default value of the `Enum` when it has a negative value. `m['k'] IN (...)` with
-- a set that contains the name of the value 0 is true for rows without the key, so a bloom filter
-- index on `mapValues` or `mapKeys` must not prune them.
DROP TABLE IF EXISTS t_map_enum_neg_values_bf;
DROP TABLE IF EXISTS t_map_enum_neg_keys_bf;
DROP TABLE IF EXISTS t_map_nullable_enum_neg_keys_bf;

CREATE TABLE t_map_enum_neg_values_bf
(
    id UInt64,
    m Map(String, Enum8('m' = -1, 'z' = 0, 'a' = 1)),
    INDEX idx_values mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_enum_neg_values_bf VALUES (1, map('x', 'a')), (2, map('k', 'a')), (3, map('k', 'm'));

SELECT id FROM t_map_enum_neg_values_bf WHERE m['k'] IN ('z') ORDER BY id;
SELECT id FROM t_map_enum_neg_values_bf WHERE m['k'] IN ('z', 'm') ORDER BY id;
SELECT id FROM t_map_enum_neg_values_bf WHERE m['k'] IN ('a') ORDER BY id;
SELECT id FROM t_map_enum_neg_values_bf WHERE m['k'] IN ('z') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_map_enum_neg_values_bf WHERE m['k'] IN ('z', 'm') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_map_enum_neg_values_bf WHERE m['k'] IN ('a') ORDER BY id SETTINGS use_skip_indexes = 0;

CREATE TABLE t_map_enum_neg_keys_bf
(
    id UInt64,
    m Map(String, Enum8('m' = -1, 'z' = 0, 'a' = 1)),
    INDEX idx_keys mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_enum_neg_keys_bf VALUES (1, map('x', 'a')), (2, map('k', 'a')), (3, map('k', 'm'));

SELECT id FROM t_map_enum_neg_keys_bf WHERE m['k'] IN ('z') ORDER BY id;
SELECT id FROM t_map_enum_neg_keys_bf WHERE m['k'] IN ('a') ORDER BY id;
SELECT id FROM t_map_enum_neg_keys_bf WHERE m['k'] IN ('z') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_map_enum_neg_keys_bf WHERE m['k'] IN ('a') ORDER BY id SETTINGS use_skip_indexes = 0;

-- Over `Nullable(Enum)` map values a missing key gives `NULL`, so the index can still prune by the name of the value 0.
CREATE TABLE t_map_nullable_enum_neg_keys_bf
(
    id UInt64,
    m Map(String, Nullable(Enum8('m' = -1, 'z' = 0, 'a' = 1))),
    INDEX idx_keys mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_nullable_enum_neg_keys_bf VALUES (1, map('x', 'a')), (2, map('k', 'a')), (3, map('k', 'z'));

SELECT id FROM t_map_nullable_enum_neg_keys_bf WHERE m['k'] IN ('z') ORDER BY id;
SELECT id FROM t_map_nullable_enum_neg_keys_bf WHERE m['k'] IN ('z') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_nullable_enum_neg_keys_bf WHERE m['k'] IN ('z') SETTINGS use_query_condition_cache = 0, parallel_replicas_local_plan = 1) WHERE explain LIKE '%Granules: %/%';

DROP TABLE t_map_enum_neg_values_bf;
DROP TABLE t_map_enum_neg_keys_bf;
DROP TABLE t_map_nullable_enum_neg_keys_bf;
