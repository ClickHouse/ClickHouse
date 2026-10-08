-- Test: ORDER BY, equality, DISTINCT, GROUP BY, min/max on bucketed Map columns.
-- These operations depend on ColumnMap::compareAt (positional comparison),
-- which is broken without the key order preservation fix.

-- Section 1: ORDER BY on Map column
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES
    (1, {'a':1, 'b':2}),
    (2, {'a':1, 'c':3}),
    (3, {'b':1, 'a':2}),
    (4, {'a':1, 'b':1});

SELECT 'S1: ORDER BY map column';
SELECT id, m FROM t ORDER BY m, id;
DROP TABLE t;

-- Section 2: Equality of semantically identical maps
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES
    (1, {'a':1, 'b':2, 'c':3}),
    (2, {'a':1, 'b':2, 'c':3});

SELECT 'S2: equal maps compare equal';
SELECT count() FROM t WHERE m = (SELECT m FROM t WHERE id = 1 LIMIT 1);
DROP TABLE t;

-- Section 3: DISTINCT on Map column
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES
    (1, {'z':1, 'a':2}),
    (2, {'z':1, 'a':2}),
    (3, {'a':2, 'z':1}),
    (4, {'z':1, 'a':3});

SELECT 'S3: DISTINCT on map column';
SELECT DISTINCT m FROM t ORDER BY m;
DROP TABLE t;

-- Section 4: GROUP BY on Map column
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES
    (1, {'z':1, 'a':2}),
    (2, {'z':1, 'a':2}),
    (3, {'x':10, 'y':20}),
    (4, {'x':10, 'y':20}),
    (5, {'x':10, 'y':20});

SELECT 'S4: GROUP BY map column';
SELECT m, count() AS cnt FROM t GROUP BY m ORDER BY cnt, m;
DROP TABLE t;

-- Section 5: min/max on Map column
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES
    (1, {'b':1, 'a':2}),
    (2, {'a':1, 'b':2}),
    (3, {'c':1});

SELECT 'S5: min/max on map column';
SELECT min(m), max(m) FROM t;
DROP TABLE t;

-- Section 6: ORDER BY after OPTIMIZE FINAL
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (1, {'a':1, 'b':2});
INSERT INTO t VALUES (2, {'a':1, 'c':3});
INSERT INTO t VALUES (3, {'b':1, 'a':2});
INSERT INTO t VALUES (4, {'a':1, 'b':1});

OPTIMIZE TABLE t FINAL;

SELECT 'S6: ORDER BY after merge';
SELECT id, m FROM t ORDER BY m, id;
DROP TABLE t;

-- Section 7: ORDER BY with UInt64 keys
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(UInt64, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES
    (1, {100:'a', 1:'b'}),
    (2, {1:'x', 100:'y'}),
    (3, {50:'z'});

SELECT 'S7: ORDER BY with UInt64 keys';
SELECT id, m FROM t ORDER BY m, id;
DROP TABLE t;

-- Section 8: Compact parts — ORDER BY correctness
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES
    (1, {'a':1, 'b':2}),
    (2, {'a':1, 'c':3}),
    (3, {'b':1, 'a':2}),
    (4, {'a':1, 'b':1});

SELECT 'S8: ORDER BY compact parts';
SELECT id, m FROM t ORDER BY m, id;
DROP TABLE t;
