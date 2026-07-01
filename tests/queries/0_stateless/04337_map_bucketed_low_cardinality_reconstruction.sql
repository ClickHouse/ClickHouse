DROP TABLE IF EXISTS t_map_lc_reconstruct;

CREATE TABLE t_map_lc_reconstruct
(
    id UInt64,
    m_key_lc Map(LowCardinality(String), String),
    m_value_lc Map(String, LowCardinality(String)),
    m_both_lc Map(LowCardinality(String), LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant',
    max_buckets_in_map = 4,
    map_buckets_min_avg_size = 0,
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1;

INSERT INTO t_map_lc_reconstruct
SELECT
    id,
    mapFromArrays(keys, vals),
    mapFromArrays(keys, vals),
    mapFromArrays(keys, vals)
FROM values(
    'id UInt64, keys Array(String), vals Array(String)',
    (1, ['alpha', 'beta', 'gamma', 'delta'], ['one', 'two', 'three', 'four']),
    (2, ['beta', 'epsilon', 'zeta'], ['twenty', 'five', 'six']),
    (3, [], []),
    (4, ['alpha', 'eta'], ['again', 'seven']));

SELECT 'full maps sorted by key';
SELECT id, mapSort(m_key_lc), mapSort(m_value_lc), mapSort(m_both_lc)
FROM t_map_lc_reconstruct
ORDER BY id;

SELECT 'keys and values subcolumns';
SELECT id, arraySort(mapKeys(m_key_lc)), arraySort(mapValues(m_value_lc)), arraySort(mapKeys(m_both_lc)), arraySort(mapValues(m_both_lc))
FROM t_map_lc_reconstruct
ORDER BY id;

SELECT 'full map together with key lookups and subcolumns';
SELECT
    id,
    m_key_lc['alpha'],
    m_value_lc['beta'],
    m_both_lc['epsilon'],
    mapSort(m_key_lc),
    arraySort(mapKeys(m_key_lc)),
    arraySort(mapValues(m_value_lc))
FROM t_map_lc_reconstruct
ORDER BY id;

DROP TABLE t_map_lc_reconstruct;
