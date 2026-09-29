-- `jsonbf_v1` skips values whose tokens are already added: a shared value equal to the previous one of its path,
-- and a pair of dictionary indexes of a `Map` with `LowCardinality` keys and values seen before in the same call.
-- Check that the values are found in every granule, also when they repeat, alternate, or reappear after other values.
SET allow_experimental_json_bloom_filter_index = 1;
DROP TABLE IF EXISTS json_bf_repeated_values;

CREATE TABLE json_bf_repeated_values
(
    id UInt64,
    j JSON(max_dynamic_paths = 0, m Map(LowCardinality(String), LowCardinality(String))),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 2
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO json_bf_repeated_values VALUES
    (1, '{"s":"a","m":{"k1":"v1","k2":"v2"},"list":[{"y":1}]}'),
    (2, '{"s":"a","m":{"k1":"v1","k2":"v2"},"list":[{"y":1}]}'),
    (3, '{"s":"b","m":{"k1":"v1"}}'),
    (4, '{"s":"a","m":{"k1":"v2"}}'),
    (5, '{"s":"a","m":{"k2":"v2","k1":"v1"}}'),
    (6, '{"s":"c","m":{}}'),
    (7, '{"s":"a","m":{"k1":"v1"}}'),
    (8, '{"s":"a","m":{"k1":"v1"},"list":[{"y":1}]}'),
    (9, '{"s":"d","m":{"k3":"v1"}}'),
    (10, '{"s":"d","m":{"k1":"v1"}}'),
    (11, '{"s":"a","m":{"k1":"v3"}}'),
    (12, '{"s":"a","m":{"k1":"v1"}}');

-- Each value is looked up with the index and without it; both results are the same.
SELECT 's a', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'a' SETTINGS force_data_skipping_indices = 'idx';
SELECT 's a', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'a' SETTINGS use_skip_indexes = 0;
SELECT 's b', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'b' SETTINGS force_data_skipping_indices = 'idx';
SELECT 's b', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'b' SETTINGS use_skip_indexes = 0;
SELECT 's c', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'c' SETTINGS force_data_skipping_indices = 'idx';
SELECT 's c', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'c' SETTINGS use_skip_indexes = 0;
SELECT 's d', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'd' SETTINGS force_data_skipping_indices = 'idx';
SELECT 's d', groupArray(id) FROM json_bf_repeated_values WHERE j.s = 'd' SETTINGS use_skip_indexes = 0;
SELECT 'm k1 v1', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k1'] = 'v1' SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
SELECT 'm k1 v1', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k1'] = 'v1' SETTINGS optimize_functions_to_subcolumns = 0, use_skip_indexes = 0;
SELECT 'm k1 v2', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k1'] = 'v2' SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
SELECT 'm k1 v2', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k1'] = 'v2' SETTINGS optimize_functions_to_subcolumns = 0, use_skip_indexes = 0;
SELECT 'm k1 v3', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k1'] = 'v3' SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
SELECT 'm k1 v3', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k1'] = 'v3' SETTINGS optimize_functions_to_subcolumns = 0, use_skip_indexes = 0;
SELECT 'm k2 v2', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k2'] = 'v2' SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
SELECT 'm k2 v2', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k2'] = 'v2' SETTINGS optimize_functions_to_subcolumns = 0, use_skip_indexes = 0;
SELECT 'm k3 v1', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k3'] = 'v1' SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
SELECT 'm k3 v1', groupArray(id) FROM json_bf_repeated_values WHERE j.m['k3'] = 'v1' SETTINGS optimize_functions_to_subcolumns = 0, use_skip_indexes = 0;
SELECT 'list y 1', groupArray(id) FROM json_bf_repeated_values WHERE has(j.list[].y, 1::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'list y 1', groupArray(id) FROM json_bf_repeated_values WHERE has(j.list[].y, 1::Int64) SETTINGS use_skip_indexes = 0;

DROP TABLE json_bf_repeated_values;
