-- Reproducer for `MergeTreeReaderWide` returning too many rows when reading
-- both `m` and `m.key_a` from a wide `Map` column across disjoint mark ranges.
-- The contiguous scan is a control query; the disjoint range forces multiple
-- `readRows` calls that append into the same result columns.

DROP TABLE IF EXISTS t_map_cache_bug;

CREATE TABLE t_map_cache_bug
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types',
    index_granularity = 700;

INSERT INTO t_map_cache_bug
SELECT
    number,
    multiIf(
        number < 9000, map('a', number, 'b', number + 1, 'c', number + 2),
        number < 18000, map('a', number, 'b', number + 1),
        number < 27000, map('c', number, 'd', number + 1),
        number < 36000, map('a', number),
        number < 42000, map('a', number, 'b', number + 1, 'c', number + 2, 'd', number + 3, 'e', number + 4),
        map('b', number))
FROM numbers(45000)
SETTINGS max_insert_threads = 1;

SELECT 'contiguous';
SELECT m, m.key_a
FROM t_map_cache_bug
FORMAT Null
SETTINGS max_threads = 1;

SELECT 'disjoint ranges';
SELECT m, m.key_a
FROM t_map_cache_bug
WHERE id < 3500 OR id >= 7000
FORMAT Null
SETTINGS max_threads = 1;

DROP TABLE t_map_cache_bug;
