-- Skip indexes and the primary key look through conversions of the indexed column that cannot change a value
-- or throw: adding or dropping `LowCardinality`, adding `Nullable` (as `toNullable`, which `join_use_nulls` emits
-- in pushed-down filters, or as `CAST`), at any depth of `Array`. The same was done for the text index before.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    n UInt32,
    s_set String,
    s_bf String,
    s_tbf String,
    s_ngram String,
    arr Array(String),
    m Map(String, LowCardinality(String)),
    INDEX idx_minmax n TYPE minmax GRANULARITY 1,
    INDEX idx_set s_set TYPE set(100) GRANULARITY 1,
    INDEX idx_bf s_bf TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tbf s_tbf TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX idx_ngram s_ngram TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_arr arr TYPE bloom_filter GRANULARITY 1,
    INDEX idx_map mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- One part with four granules of two rows.
INSERT INTO tab SELECT
    number,
    number * 10,
    concat('set', toString(number)),
    concat('bf', toString(number)),
    concat('token', toString(number), ' tail'),
    concat('ngram', toString(number)),
    [concat('elem', toString(number))],
    map('k', concat('value', toString(number)))
FROM numbers(8);

SELECT '-- primary key';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toLowCardinality(id) = 5) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toLowCardinality(id) IN (1, 5)) WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toNullable(id) > 5) WHERE explain LIKE '%Granules:%';
SELECT id FROM tab WHERE toLowCardinality(id) IN (1, 5) ORDER BY id;

SELECT '-- minmax';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toLowCardinality(n) = 50) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE CAST(n, 'Nullable(UInt32)') BETWEEN 20 AND 30) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE toLowCardinality(n) = 50 SETTINGS force_data_skipping_indices = 'idx_minmax';

SELECT '-- set';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toLowCardinality(s_set) = 'set5') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE toNullable(s_set) = 'set5' SETTINGS force_data_skipping_indices = 'idx_set';

SELECT '-- bloom_filter';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toLowCardinality(s_bf) = 'bf5') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toNullable(s_bf) IN ('bf1', 'bf5')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE toNullable(s_bf) = 'bf5' SETTINGS force_data_skipping_indices = 'idx_bf';
SELECT id FROM tab WHERE CAST(s_bf, 'LowCardinality(Nullable(String))') IN ('bf1', 'bf5') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_bf';
SELECT id FROM tab WHERE (toNullable(s_bf), id) IN (('bf1', 1), ('bf5', 4)) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_bf';
-- A NULL in the set matches no stored value.
SELECT id FROM tab WHERE toNullable(s_bf) IN ('bf5', NULL) ORDER BY id SETTINGS transform_null_in = 1;
SELECT id FROM tab WHERE toNullable(s_bf) IN ('bf5', NULL) ORDER BY id SETTINGS transform_null_in = 1, use_skip_indexes = 0;

SELECT '-- bloom_filter on Array';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE has(CAST(arr, 'Array(LowCardinality(String))'), 'elem5')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE has(CAST(arr, 'Array(Nullable(String))'), 'elem5') SETTINGS force_data_skipping_indices = 'idx_arr';
SELECT id FROM tab WHERE hasAny(CAST(arr, 'Array(LowCardinality(String))'), ['elem1', 'elem5']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_arr';

SELECT '-- bloom_filter on mapValues';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toLowCardinality(m['k']) = 'value5') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE toNullable(m['k']) = 'value5' SETTINGS force_data_skipping_indices = 'idx_map';

SELECT '-- tokenbf_v1';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasToken(toLowCardinality(s_tbf), 'token5')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE toNullable(s_tbf) = 'token5 tail' SETTINGS force_data_skipping_indices = 'idx_tbf';
SELECT id FROM tab WHERE toLowCardinality(s_tbf) IN ('token1 tail', 'token5 tail') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_tbf';

SELECT '-- ngrambf_v1';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE toNullable(s_ngram) LIKE '%ngram5%') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE CAST(s_ngram, 'LowCardinality(String)') LIKE '%ngram5%' SETTINGS force_data_skipping_indices = 'idx_ngram';

SELECT '-- join_use_nulls wraps the pushed-down filter into toNullable';
SELECT t.id FROM (SELECT 5 AS id) AS l LEFT JOIN tab AS t ON l.id = t.id WHERE t.s_bf = 'bf5'
SETTINGS join_use_nulls = 1, force_data_skipping_indices = 'idx_bf', query_plan_convert_outer_join_to_inner_join = 1;

SELECT '-- a lossy conversion is not looked through';
SELECT id FROM tab WHERE CAST(s_bf, 'FixedString(3)') = 'bf5' SETTINGS force_data_skipping_indices = 'idx_bf'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;
