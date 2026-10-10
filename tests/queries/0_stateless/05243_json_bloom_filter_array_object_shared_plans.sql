-- The plans of shared paths are reused for all objects under the same prefix while a granule is built.
-- Check that values are found when the objects of arrays change the types of their paths between elements, rows and granules.
SET allow_experimental_json_bloom_filter_index = 1;
DROP TABLE IF EXISTS json_bf_array_object_plans;

CREATE TABLE json_bf_array_object_plans
(
    id UInt64,
    j JSON(max_dynamic_paths = 0, typed Array(JSON)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO json_bf_array_object_plans VALUES
    (1, '{"arr":[{"b":1},{"b":"x1"},{"b":true}],"typed":[{"b":10}]}'),
    (2, '{"arr":[{"b":"x2"},{"b":2},{"c":{"b":3}}],"typed":[{"b":"y2"}]}'),
    (3, '{"arr":[{"b":false},{"b":4}],"typed":[{"b":30},{"b":"y3"}]}'),
    (4, '{"arr":[{"b":5}],"typed":[{"b":40}]}'),
    (5, '{"arr":[{"b":"x5"},{"b":6}],"typed":[{"b":"y5"}]}'),
    (6, '{"arr":[{"c":{"b":"x6"}}],"typed":[{"b":60}]}');

-- Each value is looked up with the index and without it; both results are the same.
SELECT 'arr 1::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 1::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr 1::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 1::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'arr 2::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 2::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr 2::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 2::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'arr 4::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 4::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr 4::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 4::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'arr 5::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 5::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr 5::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 5::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'arr 6::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 6::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr 6::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 6::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'arr x1', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 'x1') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr x1', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 'x1') SETTINGS use_skip_indexes = 0;
SELECT 'arr x2', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 'x2') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr x2', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 'x2') SETTINGS use_skip_indexes = 0;
SELECT 'arr x5', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 'x5') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'arr x5', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.arr[].b, 'x5') SETTINGS use_skip_indexes = 0;
SELECT 'typed 10::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 10::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed 10::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 10::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'typed 30::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 30::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed 30::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 30::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'typed 40::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 40::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed 40::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 40::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'typed 60::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 60::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed 60::Int64', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 60::Int64) SETTINGS use_skip_indexes = 0;
SELECT 'typed y2', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 'y2') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed y2', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 'y2') SETTINGS use_skip_indexes = 0;
SELECT 'typed y3', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 'y3') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed y3', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 'y3') SETTINGS use_skip_indexes = 0;
SELECT 'typed y5', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 'y5') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed y5', groupArray(id) FROM json_bf_array_object_plans WHERE has(j.typed[].b, 'y5') SETTINGS use_skip_indexes = 0;

DROP TABLE json_bf_array_object_plans;
