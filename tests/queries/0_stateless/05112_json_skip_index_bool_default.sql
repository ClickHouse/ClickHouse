SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS test_json_bool_default;

CREATE TABLE test_json_bool_default
(
    id UInt32,
    data JSON,
    INDEX idx_bloom JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_token JSONAllPaths(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1,
    INDEX idx_ngram JSONAllPaths(data) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- Missing paths and explicit `null` both cast to `false`, but neither contributes
-- the queried path to `JSONAllPaths`.
INSERT INTO test_json_bool_default VALUES
    (1, '{"other":"present"}'),
    (2, '{"flag":false}'),
    (3, '{"flag":true}'),
    (4, '{"flag":null}');

SELECT 'without indexes';
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = false SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = true SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.flag, 'Nullable(Bool)') = false SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false) SETTINGS use_skip_indexes = 0;

-- Select one index at a time so every shared-helper caller is exercised.
SELECT 'bloom_filter';
SET ignore_data_skipping_indices = 'idx_token,idx_ngram';
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = false;
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE 0 = data.flag::Bool;
SELECT countIf(position(explain, 'Name: idx_bloom') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE data.flag::Bool = false);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = true;
SELECT countIf(position(explain, 'Name: idx_bloom') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE data.flag::Bool = true);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.flag, 'Nullable(Bool)') = false;
SELECT countIf(position(explain, 'Name: idx_bloom') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE CAST(data.flag, 'Nullable(Bool)') = false);

-- The same default-value comparison must hold for a `Bool` nested in a tuple.
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false);
SELECT countIf(position(explain, 'Name: idx_bloom') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false));

SELECT 'tokenbf_v1';
SET ignore_data_skipping_indices = 'idx_bloom,idx_ngram';
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = false;
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE 0 = data.flag::Bool;
SELECT countIf(position(explain, 'Name: idx_token') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE data.flag::Bool = false);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = true;
SELECT countIf(position(explain, 'Name: idx_token') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE data.flag::Bool = true);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.flag, 'Nullable(Bool)') = false;
SELECT countIf(position(explain, 'Name: idx_token') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE CAST(data.flag, 'Nullable(Bool)') = false);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false);
SELECT countIf(position(explain, 'Name: idx_token') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false));

SELECT 'ngrambf_v1';
SET ignore_data_skipping_indices = 'idx_bloom,idx_token';
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = false;
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE 0 = data.flag::Bool;
SELECT countIf(position(explain, 'Name: idx_ngram') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE data.flag::Bool = false);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE data.flag::Bool = true;
SELECT countIf(position(explain, 'Name: idx_ngram') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE data.flag::Bool = true);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.flag, 'Nullable(Bool)') = false;
SELECT countIf(position(explain, 'Name: idx_ngram') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE CAST(data.flag, 'Nullable(Bool)') = false);
SELECT arraySort(groupArray(id)) FROM test_json_bool_default WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false);
SELECT countIf(position(explain, 'Name: idx_ngram') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM test_json_bool_default WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false));

DROP TABLE test_json_bool_default;
