-- Tags: no-random-merge-tree-settings
SET allow_experimental_map_key_columns = 1;
-- Tag no-random-merge-tree-settings: the randomized `map_serialization_version` replaces `with_key_columns`

-- A key that first appears after some granules of the part were written gets the rows written
-- before it as absent rows. The mutation below writes blocks of 8192 rows into granules of 1000
-- rows, so the key appears in the middle of a granule.

DROP TABLE IF EXISTS t_wkc_late_key;

CREATE TABLE t_wkc_late_key
(
    id UInt64,
    lc Map(String, LowCardinality(String)),
    lcn Map(String, LowCardinality(Nullable(String))),
    s Map(String, String),
    n Map(String, UInt64),
    arr Map(String, Array(UInt32))
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 1000,
    index_granularity_bytes = '10Mi';

INSERT INTO t_wkc_late_key
SELECT number, map('a', toString(number % 3)), map('a', toString(number % 3)), map('a', toString(number)), map('a', number), map('a', [number])
FROM numbers(30000);

ALTER TABLE t_wkc_late_key UPDATE
    lc = map('a', toString(id % 3), 'late', 'x'),
    lcn = map('a', toString(id % 3), 'late', if(id % 2 = 0, NULL, 'y')),
    s = map('a', toString(id), 'late', toString(id)),
    n = map('a', id, 'late', id),
    arr = map('a', [toUInt32(id)], 'late', [1, 2])
WHERE id >= 20500
SETTINGS mutations_sync = 2;

SELECT
    count(),
    countIf(mapContains(lc, 'late')), countIf(lc['late'] = 'x'), countIf(lc['late'] = ''),
    countIf(mapContains(lcn, 'late')), countIf(lcn['late'] IS NULL),
    countIf(s['late'] = toString(id)),
    sum(n['late']),
    countIf(arr['late'] = [1, 2])
FROM t_wkc_late_key;

SELECT id, lc, lcn, s, n, arr FROM t_wkc_late_key WHERE id IN (0, 16383, 16384, 16999, 17000, 20499, 20500, 20501, 29999) ORDER BY id;

SELECT
    countIf(lc = if(id >= 20500, map('a', toString(id % 3), 'late', 'x'), map('a', toString(id % 3)))),
    countIf(n = if(id >= 20500, map('a', id, 'late', id), map('a', id)))
FROM t_wkc_late_key SETTINGS max_threads = 8;

CHECK TABLE t_wkc_late_key SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wkc_late_key;
