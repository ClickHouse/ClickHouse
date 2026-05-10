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
-- the single in-order merging-pipe path in `splitPartsWithRangesByPrimaryKey`.

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
