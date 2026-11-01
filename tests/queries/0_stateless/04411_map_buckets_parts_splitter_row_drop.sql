-- Tags: no-object-storage
-- (no-object-storage: with_buckets writes many small per-bucket files; on S3 under ASan this
--  can time out. The splitter bug is storage-agnostic, so local-only coverage loses nothing.)

-- Regression test for parallel reads of Map primary key with with_buckets serialization.
--
-- When a Map column is the primary key and the part uses with_buckets serialization,
-- the primary key index stores Map values in insertion order, while without the key
-- order preservation fix, data files would store keys reordered by bucket index.
-- The PartsSplitter boundary calculation compares index values against actual data rows;
-- under positional ColumnMap::compareAt, mismatched key order causes
-- FilterSortedStreamByRange to drop rows.
--
-- The MapBucketIndexes fix ensures data files preserve the original key insertion order,
-- so the index and data agree and PartsSplitter works correctly.

-- Section 1: Basic Map primary key with PartsSplitter injection
DROP TABLE IF EXISTS t;

CREATE TABLE t (a Map(String, Array(UInt8)))
ENGINE = MergeTree() ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO t SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'S1: no injection';
SELECT count() FROM t
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

SELECT 'S1: with injection';
SELECT count() FROM t
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE t;

-- Section 2: ReplacingMergeTree FINAL with Map primary key
DROP TABLE IF EXISTS t;

CREATE TABLE t (a Map(String, Array(UInt8)))
ENGINE = ReplacingMergeTree() ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO t SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'S2: ReplacingMergeTree FINAL';
SELECT count() FROM t FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;

DROP TABLE t;

-- Section 3: Composite primary key (id, Map)
DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt32, m Map(String, Array(UInt8)))
ENGINE = MergeTree() ORDER BY (id, m)
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (1, map('k1', [1,2,3], 'k2', [4,5,6])), (1, map('k0', [], 'k1', [100,20,90]));
INSERT INTO t SELECT 1, map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT 1, map('k2', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'S3: composite PK with injection';
SELECT count() FROM t
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE t;

-- Section 4: Composite PK + ReplacingMergeTree FINAL
DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt32, m Map(String, Array(UInt8)))
ENGINE = ReplacingMergeTree() ORDER BY (id, m)
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (1, map('k1', [1,2,3], 'k2', [4,5,6])), (1, map('k0', [], 'k1', [100,20,90]));
INSERT INTO t SELECT 1, map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT 1, map('k2', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'S4: composite PK + FINAL';
SELECT count() FROM t FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;

DROP TABLE t;

-- Section 5: Tuple(Map, UInt32) primary key
DROP TABLE IF EXISTS t;

CREATE TABLE t (c Tuple(Map(String, Array(UInt8)), UInt32))
ENGINE = MergeTree() ORDER BY c
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (tuple(map('k1', [1,2,3], 'k2', [4,5,6]), 1)), (tuple(map('k0', [], 'k1', [100,20,90]), 1));
INSERT INTO t SELECT tuple(map('k1', [number, number + 2, number * 2]), 1) FROM numbers(6);
INSERT INTO t SELECT tuple(map('k2', [number, number + 2, number * 2]), 1) FROM numbers(6);

SELECT 'S5: Tuple(Map, UInt32) PK with injection';
SELECT count() FROM t
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE t;

-- Section 6: Tuple(Map, UInt32) PK + ReplacingMergeTree FINAL
DROP TABLE IF EXISTS t;

CREATE TABLE t (c Tuple(Map(String, Array(UInt8)), UInt32))
ENGINE = ReplacingMergeTree() ORDER BY c
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (tuple(map('k1', [1,2,3], 'k2', [4,5,6]), 1)), (tuple(map('k0', [], 'k1', [100,20,90]), 1));
INSERT INTO t SELECT tuple(map('k1', [number, number + 2, number * 2]), 1) FROM numbers(6);
INSERT INTO t SELECT tuple(map('k2', [number, number + 2, number * 2]), 1) FROM numbers(6);

SELECT 'S6: Tuple(Map, UInt32) PK + FINAL';
SELECT count() FROM t FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;

DROP TABLE t;

-- Section 7: mapKeys(m) as primary key
DROP TABLE IF EXISTS t;

CREATE TABLE t (m Map(String, Array(UInt8)))
ENGINE = MergeTree() ORDER BY mapKeys(m)
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO t SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT map('k3', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'S7: mapKeys PK with injection';
SELECT count() FROM t
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE t;

-- Section 8: m.keys as primary key
DROP TABLE IF EXISTS t;

CREATE TABLE t (m Map(String, Array(UInt8)))
ENGINE = MergeTree() ORDER BY m.keys
SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2,
    serialization_info_version = 'with_types';

INSERT INTO t VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO t SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO t SELECT map('k3', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'S8: m.keys PK with injection';
SELECT count() FROM t
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE t;
