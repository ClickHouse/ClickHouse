SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_path_values_dynamic_arrays;
CREATE TABLE json_path_values_dynamic_arrays
(
    id UInt64,
    data JSON(max_dynamic_paths = 16, max_dynamic_types = 16),
    shared JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX shared_tokens shared TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_path_values_dynamic_arrays;
INSERT INTO json_path_values_dynamic_arrays VALUES
    (1,
     '{"tags":["foo",""],"cast_tags":["1","2"],"mixed":["foo"]}',
     '{"tags":["foo",""]}'),
    (2,
     concat('{"tags":["bar","', repeat('x', 100), '"],"cast_tags":[1,2],"mixed":"not-an-array"}'),
     '{"tags":["bar"]}'),
    (3,
     '{"tags":[1,2],"cast_tags":["3"],"mixed":["bar"]}',
     '{"tags":[1,2]}'),
    (4,
     '{"tags":[null,"foo"]}',
     '{"tags":[null,"foo"]}'),
    (5, '{}', '{}');

SELECT 'exact dynamic array types';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, 'foo')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, '');
SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, '')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, repeat('x', 100))
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(Int64))`, 2)
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, CAST(NULL, 'Nullable(String)'));
SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, CAST(NULL, 'Nullable(String)'))
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'shared dynamic arrays';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(shared.tags.:`Array(Nullable(String))`, 'foo')
SETTINGS force_data_skipping_indices = 'shared_tokens';

SELECT 'converting casts';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(Int64))), 2)
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.mixed AS Array(Nullable(String))), 'foo')
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.mixed AS Array(Nullable(String))), 'foo')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'planner';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_path_values_dynamic_arrays
    WHERE has(data.tags.:`Array(Nullable(String))`, 'foo')
)
WHERE position(explain, '__text_index') > 0;
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_path_values_dynamic_arrays
    WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
)
WHERE position(explain, '__text_index') > 0;

SYSTEM START MERGES json_path_values_dynamic_arrays;
OPTIMIZE TABLE json_path_values_dynamic_arrays FINAL;

SELECT 'merged';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, 'foo')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE json_path_values_dynamic_arrays;
