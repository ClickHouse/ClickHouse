-- Element access of a Map with Enum keys by a constant name is optimized
-- to a key subcolumn read (which prunes buckets in the bucketed Map serialization).

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_enum_buckets;

CREATE TABLE t_map_enum_buckets
(
    id UInt64,
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), Int64),
    va Int64 ALIAS m['a']
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0;

INSERT INTO t_map_enum_buckets VALUES (1, map('a', 10, 'c', 30)), (2, map('b', 20)), (3, map());

SELECT m['a'], m['b'], m['c'] FROM t_map_enum_buckets ORDER BY id;
SELECT va FROM t_map_enum_buckets ORDER BY id;
SELECT m.key_a FROM t_map_enum_buckets ORDER BY id;

-- The element access by a constant name reads a key subcolumn instead of the whole map.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT m['a'] FROM t_map_enum_buckets) WHERE explain LIKE '%key\_a%';

DROP TABLE t_map_enum_buckets;
