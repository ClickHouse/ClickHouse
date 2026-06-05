-- Regression test for parallel reads of `Map` primary key with `with_buckets` serialization.
--
-- Background: when a `Map` column is the primary key and the part uses
-- `map_serialization_version_for_zero_level_parts = 'with_buckets'`, the primary key
-- index stores Map values in their in-memory insertion order (via `SerializationMap::serializeBinary`,
-- which uses the BASIC nested `Array(Tuple(K, V))` serialization), while the data files
-- store the same rows with per-row keys reordered by ascending bucket index.
-- Reading the data back produces Map values whose internal key order differs from the
-- index. The `PartsSplitter` boundary calculation (used by `splitPartsWithRangesByPrimaryKey`
-- for both the layer-split path and the intersecting / non-intersecting separation under FINAL)
-- compares index values against actual data rows; under positional `ColumnMap::compareAt`
-- these representations are not equal, and `FilterSortedStreamByRange` drops rows that should
-- fall inside a layer's PK range.
--
-- The fix treats `Map` as not "safe" for the splitter, so `Map`-keyed reads always take
-- the single in-order merging-pipe path in `splitPartsWithRangesByPrimaryKey`. The check is
-- recursive — see `isSafePrimaryDataKeyType` — so the same fix also covers composite primary
-- keys with `Map` columns and primary keys whose type is a `Tuple` containing a `Map`.

DROP TABLE IF EXISTS tm_split_repro;

CREATE TABLE tm_split_repro(a Map(String, Array(UInt8))) ENGINE = MergeTree() ORDER BY a SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

-- 14 rows across 3 parts; the row inserted with multiple keys is the one that used to be dropped.
INSERT INTO tm_split_repro VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO tm_split_repro SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);

-- Without injection: control. Always 14.
SELECT 'no-inj count', count() FROM tm_split_repro
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SELECT 'no-inj groupArray', length(groupArray(a)) FROM tm_split_repro
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- With injection forced on: previously 13 because of the splitter-layer / boundary mismatch.
-- After the fix, `Map` is treated as not a safe primary key for the splitter, so the
-- intersecting / non-intersecting split and layer split are bypassed and all 14 rows are returned.
SELECT 'inj count', count() FROM tm_split_repro
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;
SELECT 'inj groupArray', length(groupArray(a)) FROM tm_split_repro
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE tm_split_repro;

-- Same shape but on a `ReplacingMergeTree` exercising the FINAL path with the
-- `split_parts_ranges_into_intersecting_and_non_intersecting_final = 0` setting, which
-- forces all parts through the layer split. Previously 13; after the fix, 14.

DROP TABLE IF EXISTS tm_split_repro_final;

CREATE TABLE tm_split_repro_final(a Map(String, Array(UInt8))) ENGINE = ReplacingMergeTree() ORDER BY a SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

INSERT INTO tm_split_repro_final VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO tm_split_repro_final SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_final SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'final-split-0 count', count() FROM tm_split_repro_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;
SELECT 'final-split-0 groupArray', length(groupArray(a)) FROM tm_split_repro_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;

DROP TABLE tm_split_repro_final;

-- Composite primary key `(id, m)` where `m` is a `Map`. This exercises the loop in
-- `isSafePrimaryKey` that scans every PK column individually, so the splitter must mark
-- the whole PK as unsafe as soon as any column is a `Map`. All rows use the same `id`
-- to make every part intersect on `id`, which forces the splitter's boundary calculation
-- to compare `m` values across parts.

DROP TABLE IF EXISTS tm_split_repro_composite;

CREATE TABLE tm_split_repro_composite(id UInt32, m Map(String, Array(UInt8))) ENGINE = MergeTree() ORDER BY (id, m) SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

INSERT INTO tm_split_repro_composite VALUES (1, map('k1', [1,2,3], 'k2', [4,5,6])), (1, map('k0', [], 'k1', [100,20,90]));
INSERT INTO tm_split_repro_composite SELECT 1, map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_composite SELECT 1, map('k2', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'composite-inj count', count() FROM tm_split_repro_composite
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;
SELECT 'composite-inj groupArray', length(groupArray(m)) FROM tm_split_repro_composite
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE tm_split_repro_composite;

-- Same composite PK shape on `ReplacingMergeTree`, exercising the FINAL split path.
-- Without the fix this previously returned 13 rows for both `count` and `groupArray`.

DROP TABLE IF EXISTS tm_split_repro_composite_final;

CREATE TABLE tm_split_repro_composite_final(id UInt32, m Map(String, Array(UInt8))) ENGINE = ReplacingMergeTree() ORDER BY (id, m) SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

INSERT INTO tm_split_repro_composite_final VALUES (1, map('k1', [1,2,3], 'k2', [4,5,6])), (1, map('k0', [], 'k1', [100,20,90]));
INSERT INTO tm_split_repro_composite_final SELECT 1, map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_composite_final SELECT 1, map('k2', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'composite-final-split-0 count', count() FROM tm_split_repro_composite_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;
SELECT 'composite-final-split-0 groupArray', length(groupArray(m)) FROM tm_split_repro_composite_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;

DROP TABLE tm_split_repro_composite_final;

-- Primary key whose type is itself a `Tuple` containing a `Map`. This exercises the
-- recursive `Tuple` branch of `isSafePrimaryDataKeyType`, which must descend into the
-- tuple's elements and mark the whole tuple as unsafe when any element is a `Map`.

DROP TABLE IF EXISTS tm_split_repro_tuple;

CREATE TABLE tm_split_repro_tuple(c Tuple(Map(String, Array(UInt8)), UInt32)) ENGINE = MergeTree() ORDER BY c SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

INSERT INTO tm_split_repro_tuple VALUES (tuple(map('k1', [1,2,3], 'k2', [4,5,6]), 1)), (tuple(map('k0', [], 'k1', [100,20,90]), 1));
INSERT INTO tm_split_repro_tuple SELECT tuple(map('k1', [number, number + 2, number * 2]), 1) FROM numbers(6);
INSERT INTO tm_split_repro_tuple SELECT tuple(map('k2', [number, number + 2, number * 2]), 1) FROM numbers(6);

SELECT 'tuple-inj count', count() FROM tm_split_repro_tuple
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;
SELECT 'tuple-inj groupArray', length(groupArray(c)) FROM tm_split_repro_tuple
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE tm_split_repro_tuple;

-- Same `Tuple(Map, ...)` PK shape on `ReplacingMergeTree`, exercising the FINAL split path.

DROP TABLE IF EXISTS tm_split_repro_tuple_final;

CREATE TABLE tm_split_repro_tuple_final(c Tuple(Map(String, Array(UInt8)), UInt32)) ENGINE = ReplacingMergeTree() ORDER BY c SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

INSERT INTO tm_split_repro_tuple_final VALUES (tuple(map('k1', [1,2,3], 'k2', [4,5,6]), 1)), (tuple(map('k0', [], 'k1', [100,20,90]), 1));
INSERT INTO tm_split_repro_tuple_final SELECT tuple(map('k1', [number, number + 2, number * 2]), 1) FROM numbers(6);
INSERT INTO tm_split_repro_tuple_final SELECT tuple(map('k2', [number, number + 2, number * 2]), 1) FROM numbers(6);

SELECT 'tuple-final-split-0 count', count() FROM tm_split_repro_tuple_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;
SELECT 'tuple-final-split-0 groupArray', length(groupArray(c)) FROM tm_split_repro_tuple_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;

DROP TABLE tm_split_repro_tuple_final;

-- Primary key derived from a `Map` column via a function call (`mapKeys(m)`). The resolved
-- expression type is `Array(K)`, so `isSafePrimaryDataKeyType` alone cannot tell that the
-- PK depends on a `Map`. The fix walks the PK expression's source storage columns and marks
-- the PK unsafe when any source column is a `Map`. Without this extra check the splitter
-- still drops one row from a 4-part bucketed-`Map` table because the index keys (built from
-- in-memory insertion-ordered `Map`) and the data keys (read in bucket order) differ.

DROP TABLE IF EXISTS tm_split_repro_mapkeys;

CREATE TABLE tm_split_repro_mapkeys(m Map(String, Array(UInt8))) ENGINE = MergeTree() ORDER BY mapKeys(m) SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

-- 4 parts (the original 14-row 3-part shape was not enough to drop a row when the PK is
-- a `mapKeys(m)` expression; tiandiwonder's empirical confirmation used 4 parts).
INSERT INTO tm_split_repro_mapkeys VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO tm_split_repro_mapkeys SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_mapkeys SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_mapkeys SELECT map('k3', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'mapkeys-inj count', count() FROM tm_split_repro_mapkeys
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;
SELECT 'mapkeys-inj groupArray', length(groupArray(m)) FROM tm_split_repro_mapkeys
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE tm_split_repro_mapkeys;

-- Primary key as a `Map` sub-column reference (`m.keys`). The resolved expression type is
-- the sub-column type `Array(K)`, so this is also invisible to `isSafePrimaryDataKeyType`.
-- The PK source-column check resolves `m.keys` against the storage columns and finds it as
-- a sub-column of a `Map`, marking the PK unsafe.

DROP TABLE IF EXISTS tm_split_repro_mdotkeys;

CREATE TABLE tm_split_repro_mdotkeys(m Map(String, Array(UInt8))) ENGINE = MergeTree() ORDER BY m.keys SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

INSERT INTO tm_split_repro_mdotkeys VALUES (map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
INSERT INTO tm_split_repro_mdotkeys SELECT map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_mdotkeys SELECT map('k2', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_mdotkeys SELECT map('k3', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'mdotkeys-inj count', count() FROM tm_split_repro_mdotkeys
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;
SELECT 'mdotkeys-inj groupArray', length(groupArray(m)) FROM tm_split_repro_mdotkeys
    SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1, max_threads = 4;

DROP TABLE tm_split_repro_mdotkeys;

-- Same `Map`-derived PK shape on `ReplacingMergeTree`, exercising the FINAL split path with
-- `split_parts_ranges_into_intersecting_and_non_intersecting_final = 0`. Add an `id` column
-- to the PK to make every row's sort key unique so `FINAL` does not deduplicate on
-- `mapKeys(m)` collisions.

DROP TABLE IF EXISTS tm_split_repro_mapkeys_final;

CREATE TABLE tm_split_repro_mapkeys_final(id UInt32, m Map(String, Array(UInt8))) ENGINE = ReplacingMergeTree() ORDER BY (id, mapKeys(m)) SETTINGS
    min_bytes_for_wide_part = 0,
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 11,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 2;

INSERT INTO tm_split_repro_mapkeys_final VALUES (1, map('k1', [1,2,3], 'k2', [4,5,6])), (2, map('k0', [], 'k1', [100,20,90]));
INSERT INTO tm_split_repro_mapkeys_final SELECT 100 + number, map('k1', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_mapkeys_final SELECT 200 + number, map('k2', [number, number + 2, number * 2]) FROM numbers(6);
INSERT INTO tm_split_repro_mapkeys_final SELECT 300 + number, map('k3', [number, number + 2, number * 2]) FROM numbers(6);

SELECT 'mapkeys-final-split-0 count', count() FROM tm_split_repro_mapkeys_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;
SELECT 'mapkeys-final-split-0 groupArray', length(groupArray(m)) FROM tm_split_repro_mapkeys_final FINAL
    SETTINGS max_threads = 4, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;

DROP TABLE tm_split_repro_mapkeys_final;
