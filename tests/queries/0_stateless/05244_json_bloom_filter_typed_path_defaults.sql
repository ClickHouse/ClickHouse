-- `jsonbf_v1` does not index the defaults of typed paths, which they have in rows where the paths are absent,
-- and it skips a string equal to the one in the previous row.
SET allow_experimental_json_bloom_filter_index = 1;
DROP TABLE IF EXISTS json_bf_typed_defaults;

CREATE TABLE json_bf_typed_defaults
(
    id UInt64,
    j JSON(s String, n UInt64, ns Nullable(String)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Granules: [1, 4] have only defaults of `s` and `n`, [5, 8] repeat values, [9, 12] have a value after the defaults.
INSERT INTO json_bf_typed_defaults VALUES
    (1, '{"x":1}'), (2, '{"x":2}'), (3, '{"ns":""}'), (4, '{"x":4}'),
    (5, '{"s":"a","n":7}'), (6, '{"s":"a","n":7}'), (7, '{"s":"b","n":8}'), (8, '{"s":"a","n":7}'),
    (9, '{}'), (10, '{}'), (11, '{"s":"c","n":9,"ns":"v"}'), (12, '{}');

-- Values are found and prune the granules without them.
SELECT 's = a', groupArray(id) FROM json_bf_typed_defaults WHERE j.s = 'a' SETTINGS force_data_skipping_indices = 'idx';
SELECT 's = b', groupArray(id) FROM json_bf_typed_defaults WHERE j.s = 'b' SETTINGS force_data_skipping_indices = 'idx';
SELECT 's = c', groupArray(id) FROM json_bf_typed_defaults WHERE j.s = 'c' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'n = 7', groupArray(id) FROM json_bf_typed_defaults WHERE j.n = 7 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'n = 9', groupArray(id) FROM json_bf_typed_defaults WHERE j.n = 9 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'ns = v', groupArray(id) FROM json_bf_typed_defaults WHERE j.ns = 'v' SETTINGS force_data_skipping_indices = 'idx';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM json_bf_typed_defaults WHERE j.s = 'c') WHERE explain LIKE '%Granules:%';

-- A default value is not indexed, so the index is not used for it, and the rows where the path is absent are found.
SELECT 's = empty', groupArray(id) FROM json_bf_typed_defaults WHERE j.s = '';
SELECT 'n = 0', groupArray(id) FROM json_bf_typed_defaults WHERE j.n = 0;
SELECT 's in', groupArray(id) FROM json_bf_typed_defaults WHERE j.s IN ('', 'c');
SELECT groupArray(id) FROM json_bf_typed_defaults WHERE j.s = '' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_bf_typed_defaults WHERE j.n = 0 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A condition on another path still prunes the granules.
SELECT 's = empty and ns = empty', groupArray(id) FROM json_bf_typed_defaults WHERE j.s = '' AND j.ns = '' SETTINGS force_data_skipping_indices = 'idx';

-- In a `Nullable` typed path, the absent value is `NULL`, and an empty string is an indexed value.
SELECT 'ns = empty', groupArray(id) FROM json_bf_typed_defaults WHERE j.ns = '' SETTINGS force_data_skipping_indices = 'idx';

-- The same results without the index.
SELECT 'no index', groupArray(id) FROM json_bf_typed_defaults WHERE j.s = 'a' OR j.n = 9 OR j.s = '' SETTINGS use_skip_indexes = 0;
SELECT 'with index', groupArray(id) FROM json_bf_typed_defaults WHERE j.s = 'a' OR j.n = 9 OR j.s = '';

DROP TABLE json_bf_typed_defaults;
