-- Tags: no-random-settings, no-random-merge-tree-settings

-- One Compact part, several granules, one INSERT block. Key set is frozen from
-- that block; every granule must keep the same substreams.

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_wkc_compact_mg;

CREATE TABLE t_wkc_compact_mg
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
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2;

INSERT INTO t_wkc_compact_mg VALUES
    (1, map('a', 1)),
    (2, map('a', 2, 'b', 10)),
    (3, map('b', 0)),
    (4, map()),
    (5, map('c', 5)),
    (6, map('a', 6, 'c', 7));

SELECT 'part';
SELECT part_type, rows, marks
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_compact_mg' AND active;

SELECT 'roundtrip';
SELECT
    id,
    mapSort(m),
    m['a'],
    m['b'],
    m['c'],
    m.exists_a,
    m.exists_b,
    m.exists_c
FROM t_wkc_compact_mg
ORDER BY id;

SELECT 'prewhere present';
SELECT id, m['a']
FROM t_wkc_compact_mg
PREWHERE m.exists_a = 1
ORDER BY id;

SELECT 'prewhere absent';
SELECT id, m['c']
FROM t_wkc_compact_mg
PREWHERE m.exists_a = 0
ORDER BY id;

SELECT 'where key value';
SELECT id, m['b']
FROM t_wkc_compact_mg
WHERE m.exists_b = 1
ORDER BY id;

CHECK TABLE t_wkc_compact_mg SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wkc_compact_mg;
