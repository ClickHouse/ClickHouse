DROP TABLE IF EXISTS json_bf_shared_scalars;
CREATE TABLE json_bf_shared_scalars
(
    id UInt64,
    j JSON(max_dynamic_paths = 0),
    INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Repeated shared paths use different types, roles and values across granules.
INSERT INTO json_bf_shared_scalars
SELECT number, ('{"s":"value-' || toString(number) || '","x":' || if(number % 2, toString(number), '"' || toString(number) || '"')
    || ',"a":["item-' || toString(number) || '","common"],"nested":{"s":"nested-' || toString(number) || '"}}')::JSON(max_dynamic_paths = 0)
FROM numbers(512);
INSERT INTO json_bf_shared_scalars VALUES (512, '{"s":"","x":true}'), (513, '{"s":"é雪","x":null}'), (514, '{}');
SELECT JSONSharedDataPaths(j), JSONDynamicPaths(j) FROM json_bf_shared_scalars WHERE id = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-0' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-0' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-127' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-127' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-128' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-128' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-511' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'value-511' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = '' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = '' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'é雪' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'é雪' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'missing' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.s = 'missing' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.x.:Int64 = 129 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.x.:Int64 = 129 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.x.:String = '128' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.x.:String = '128' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.x.:Bool = true SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.x.:Bool = true SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE has(j.a.:`Array(Nullable(String))`, 'item-128') SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE has(j.a.:`Array(Nullable(String))`, 'item-128') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.nested.s = 'nested-256' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_shared_scalars WHERE j.nested.s = 'nested-256' SETTINGS use_skip_indexes = 0;
SELECT count() FROM json_bf_shared_scalars WHERE isNotNull(j.s) SETTINGS force_data_skipping_indices = 'bf';
SELECT count() FROM json_bf_shared_scalars WHERE isNotNull(j.s) SETTINGS use_skip_indexes = 0;
DROP TABLE json_bf_shared_scalars;
