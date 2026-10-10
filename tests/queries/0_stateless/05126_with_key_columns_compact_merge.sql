-- Tags: no-random-settings, no-random-merge-tree-settings

-- Compact sources may merge; the output part must be Wide. Every source part
-- uses the same key set so this test does not depend on Wide first-block key
-- freeze (that union is a separate Wide-path issue).

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_wkc_compact_merge;
DROP TABLE IF EXISTS t_wkc_compact_mix;

SELECT 'compact parts merge to wide';
CREATE TABLE t_wkc_compact_merge
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

SYSTEM STOP MERGES t_wkc_compact_merge;
INSERT INTO t_wkc_compact_merge VALUES (1, map('a', 1, 'b', 0, 'c', 0)), (2, map('a', 2, 'b', 10, 'c', 0));
INSERT INTO t_wkc_compact_merge VALUES (3, map('a', 0, 'b', 3, 'c', 4)), (4, map('a', 0, 'b', 0, 'c', 5));
INSERT INTO t_wkc_compact_merge VALUES (5, map('a', 6, 'b', 0, 'c', 0));

SELECT arraySort(groupArray(part_type))
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_compact_merge' AND active;

SYSTEM START MERGES t_wkc_compact_merge;
OPTIMIZE TABLE t_wkc_compact_merge FINAL;

SELECT DISTINCT part_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_compact_merge' AND active;

SELECT
    id,
    mapSort(m),
    m['a'],
    m['b'],
    m['c'],
    m.exists_a,
    m.exists_b,
    m.exists_c
FROM t_wkc_compact_merge
ORDER BY id;

CHECK TABLE t_wkc_compact_merge SETTINGS check_query_single_value_result = 1;

SELECT 'compact and wide coexist in one merge';
CREATE TABLE t_wkc_compact_mix
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

SYSTEM STOP MERGES t_wkc_compact_mix;
INSERT INTO t_wkc_compact_mix VALUES (1, map('a', 1, 'b', 2, 'c', 0, 'd', 0)), (2, map('a', 0, 'b', 3, 'c', 0, 'd', 0));

ALTER TABLE t_wkc_compact_mix
    MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_wkc_compact_mix VALUES (3, map('a', 0, 'b', 4, 'c', 5, 'd', 0)), (4, map('a', 0, 'b', 0, 'c', 0, 'd', 6));

SELECT part_type, count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_compact_mix' AND active
GROUP BY part_type
ORDER BY part_type;

SYSTEM START MERGES t_wkc_compact_mix;
OPTIMIZE TABLE t_wkc_compact_mix FINAL;

SELECT DISTINCT part_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_compact_mix' AND active;

SELECT
    id,
    mapSort(m),
    m['a'],
    m['b'],
    m['c'],
    m['d'],
    m.exists_a,
    m.exists_d
FROM t_wkc_compact_mix
ORDER BY id;

CHECK TABLE t_wkc_compact_mix SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wkc_compact_merge;
DROP TABLE t_wkc_compact_mix;
