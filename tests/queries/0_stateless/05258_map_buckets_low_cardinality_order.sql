-- Reading `LowCardinality` keys and values of a `Map` with buckets restores the original order of the pairs.
-- The result is compared with the same data in a table with the basic `Map` serialization.

DROP TABLE IF EXISTS t_basic;
DROP TABLE IF EXISTS t_buckets;

CREATE TABLE t_basic
(
    id UInt64,
    m1 Map(LowCardinality(String), LowCardinality(String)),
    m2 Map(LowCardinality(String), UInt64),
    m3 Map(String, LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_buckets AS t_basic
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', max_buckets_in_map = 8, map_buckets_min_avg_size = 0;

INSERT INTO t_basic
SELECT
    number,
    mapFromArrays(keys, arrayMap(x -> toString((number + x) % 5), range(length(keys)))),
    mapFromArrays(keys, arrayMap(x -> number + x, range(length(keys)))),
    mapFromArrays(keys, arrayMap(x -> if((number + x) % 6 = 0, NULL, toString((number + x) % 5)), range(length(keys))))
FROM
(
    SELECT number, arrayMap(x -> if(x = 0 AND number % 11 = 0, '', 'k' || toString((number * 7 + x * 131) % 1000)), range(number % 20)) AS keys
    FROM numbers(3000)
);

-- Small dictionaries make the buckets switch dictionaries and use additional keys.
INSERT INTO t_buckets SELECT * FROM t_basic SETTINGS low_cardinality_max_dictionary_size = 50;

SELECT 'full';
SELECT (SELECT sum(cityHash64(id, toString(m1), toString(m2), toString(m3))) FROM t_basic)
     = (SELECT sum(cityHash64(id, toString(m1), toString(m2), toString(m3))) FROM t_buckets);
SELECT (SELECT sum(cityHash64(id, toString(m1), toString(m2), toString(m3))) FROM t_basic)
     = (SELECT sum(cityHash64(id, toString(m1), toString(m2), toString(m3))) FROM t_buckets SETTINGS max_block_size = 7);

SELECT 'subcolumns';
SELECT (SELECT sum(cityHash64(id, m1.keys, m1.values, m2.keys, m3.values)) FROM t_basic)
     = (SELECT sum(cityHash64(id, m1.keys, m1.values, m2.keys, m3.values)) FROM t_buckets);
SELECT (SELECT sum(cityHash64(id, m1.keys, m3.values, toString(m1))) FROM t_basic)
     = (SELECT sum(cityHash64(id, m1.keys, m3.values, toString(m1))) FROM t_buckets);

SELECT 'ranges';
SELECT (SELECT sum(cityHash64(id, toString(m1), m3.values)) FROM t_basic WHERE id BETWEEN 150 AND 450 OR id BETWEEN 1510 AND 1600 OR id > 2950)
     = (SELECT sum(cityHash64(id, toString(m1), m3.values)) FROM t_buckets WHERE id BETWEEN 150 AND 450 OR id BETWEEN 1510 AND 1600 OR id > 2950);

SELECT 'rows';
SELECT id, m1, m3 FROM t_buckets WHERE id IN (0, 11, 13) ORDER BY id;

DROP TABLE t_basic;
DROP TABLE t_buckets;
