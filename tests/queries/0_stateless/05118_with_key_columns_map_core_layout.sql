-- Tags: no-random-settings, no-random-merge-tree-settings
-- Pins the with_key_columns write/read core path: Wide layout, per-key streams,
-- lookup / exists_ / full Map round-trip, and arrayElement -> key_ subcolumn rewrite.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_wkc_layout;

CREATE TABLE t_wkc_layout
(
    id UInt64,
    m Map(String, UInt64),
    s Map(String, String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO t_wkc_layout VALUES
    (1, {'a': 1, 'b': 2, 'a.b': 3}, {'hot': 'x', 'cold': 'yyyy'}),
    (2, {'a': 4}, {'hot': 'z'}),
    (3, {}, {});

SELECT 'part_type';
SELECT part_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_layout' AND active
ORDER BY name;

SELECT 'm_has_core_streams';
SELECT
    has(substreams, 'm.keys_info'),
    has(substreams, 'm.key_a'),
    has(substreams, 'm.key_b'),
    has(substreams, 'm.key_presence'),
    arrayExists(x -> x LIKE '%key_a%b%' OR x LIKE '%key_a.b%', substreams)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wkc_layout' AND column = 'm' AND active;

SELECT 's_has_core_streams';
SELECT
    has(substreams, 's.keys_info'),
    has(substreams, 's.key_hot'),
    has(substreams, 's.key_cold'),
    has(substreams, 's.key_presence')
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wkc_layout' AND column = 's' AND active;

SELECT 'roundtrip';
SELECT
    id,
    m['a'],
    m['b'],
    m['a.b'],
    m['missing'],
    m.key_a,
    m.exists_a,
    m.exists_missing,
    mapSort(m),
    s['hot'],
    s['cold']
FROM t_wkc_layout
ORDER BY id;

SELECT 'types';
SELECT toTypeName(m['a']), toTypeName(m.exists_a), toTypeName(s['hot'])
FROM t_wkc_layout
LIMIT 1;

SELECT 'rewrite_m_a';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT m['a'] FROM t_wkc_layout)
WHERE explain LIKE '%m.key_a%';

SELECT 'opt0_vs_opt1';
SELECT count()
FROM
(
    SELECT id, m['a'], m['missing'], s['hot']
    FROM t_wkc_layout
    SETTINGS optimize_functions_to_subcolumns = 1
    EXCEPT ALL
    SELECT id, m['a'], m['missing'], s['hot']
    FROM t_wkc_layout
    SETTINGS optimize_functions_to_subcolumns = 0
);

DROP TABLE t_wkc_layout;
