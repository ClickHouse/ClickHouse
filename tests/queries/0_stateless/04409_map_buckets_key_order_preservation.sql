-- Test: Bucketed Map serialization preserves original key insertion order.
-- The fix writes a MapBucketIndexes substream to restore key ordering during deserialization.

-- Section 1: Wide parts, String keys — order preserved after insert
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
    (1, {'z':1, 'a':2, 'm':3, 'b':4}),
    (2, {'dog':10, 'ant':20, 'cat':30, 'bat':40}),
    (3, {'3':100, '1':200, '2':300});

SELECT 'S1: key order preserved in wide parts after insert';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;

-- Section 2: Compact parts, String keys — order preserved after insert
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
    (1, {'z':1, 'a':2, 'm':3, 'b':4}),
    (2, {'dog':10, 'ant':20, 'cat':30, 'bat':40}),
    (3, {'3':100, '1':200, '2':300});

SELECT 'S2: key order preserved in compact parts after insert';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;

-- Section 3: Wide parts — order preserved after OPTIMIZE FINAL
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

INSERT INTO t VALUES (1, {'z':1, 'a':2, 'm':3});
INSERT INTO t VALUES (2, {'x':10, 'b':20, 'w':30});
OPTIMIZE TABLE t FINAL;

SELECT 'S3: key order preserved after merge';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;

-- Section 4: UInt64 keys — order preserved
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
    (1, {100:'a', 1:'b', 50:'c', 25:'d'}),
    (2, {999:'x', 0:'y', 500:'z'});

SELECT 'S4: UInt64 key order preserved';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;

-- Section 5: Int32 keys — order preserved (signed type)
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(Int32, String))
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

INSERT INTO t VALUES (1, {-10:'a', 5:'b', -100:'c', 0:'d'});

SELECT 'S5: Int32 key order preserved';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;

-- Section 6: Bucket indexes stream presence in system.parts_columns
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

INSERT INTO t VALUES (1, {'z':1, 'a':2, 'm':3, 'b':4});

SELECT 'S6: bucket_indexes stream present';
SELECT has(substreams, 'm.bucket_indexes') AS has_bucket_indexes
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;
DROP TABLE t;

-- Section 7: No bucket_indexes when only 1 bucket (min_avg_size threshold)
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'sqrt',
    map_buckets_min_avg_size = 32,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (1, {'a':1, 'b':2, 'c':3});

SELECT 'S7: no bucket_indexes with 1 bucket';
SELECT has(substreams, 'm.bucket_indexes') AS has_bucket_indexes
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;
DROP TABLE t;

-- Section 8: Order preserved across basic->with_buckets merge
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (1, {'z':1, 'a':2, 'm':3});

ALTER TABLE t MODIFY SETTING
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0;

INSERT INTO t VALUES (2, {'x':10, 'b':20, 'w':30});

OPTIMIZE TABLE t FINAL;

SELECT 'S8: key order preserved after basic+with_buckets merge';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;

-- Section 9: Order preserved with zero-level basic, merged with_buckets
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    index_granularity = 8192,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (1, {'z':1, 'a':2}), (2, {'x':10, 'b':20});
INSERT INTO t VALUES (3, {'m':100, 'c':200});

OPTIMIZE TABLE t FINAL;

SELECT 'S9: order after zero-level=basic merged=with_buckets';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;

-- Section 10: Multiple merges preserve order
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

INSERT INTO t VALUES (1, {'z':1, 'a':2, 'm':3});
INSERT INTO t VALUES (2, {'x':10, 'b':20, 'w':30});
INSERT INTO t VALUES (3, {'q':100, 'j':200, 'p':300});
INSERT INTO t VALUES (4, {'d':1000, 'f':2000, 'e':3000});

OPTIMIZE TABLE t FINAL;

SELECT 'S10: order preserved after multiple merges';
SELECT id, mapKeys(m) FROM t ORDER BY id;
DROP TABLE t;
