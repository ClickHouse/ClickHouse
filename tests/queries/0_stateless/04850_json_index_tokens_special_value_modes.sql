SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_path_values_special_values;
CREATE TABLE json_path_values_special_values
(
    id UInt64,
    data JSON(s String, tags Array(Nullable(String))),
    dynamic JSON(max_dynamic_paths = 16, max_dynamic_types = 16),
    shared JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX dynamic_tokens dynamic TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX shared_tokens shared TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_path_values_special_values;
INSERT INTO json_path_values_special_values VALUES
    (1, '{"s":"","tags":["",null,"x"]}', '{"s":"","tags":["",null,"x"]}', '{"s":"","tags":["",null,"x"]}'),
    (2, '{"s":"x","tags":["x"]}', '{"s":"x","tags":["x"]}', '{"s":"x","tags":["x"]}'),
    (3, '{}', '{}', '{}');

SELECT arraySort(groupArray(id)) FROM json_path_values_special_values WHERE data.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() FROM json_path_values_special_values WHERE data.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id) FROM json_path_values_special_values WHERE has(data.tags, '');
SELECT count() FROM json_path_values_special_values WHERE has(data.tags, '')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id) FROM json_path_values_special_values
WHERE has(data.tags, CAST(NULL, 'Nullable(String)'));
SELECT count() FROM json_path_values_special_values
WHERE has(data.tags, CAST(NULL, 'Nullable(String)'))
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id) FROM json_path_values_special_values WHERE dynamic.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() FROM json_path_values_special_values WHERE dynamic.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'dynamic_tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id) FROM json_path_values_special_values WHERE shared.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() FROM json_path_values_special_values WHERE shared.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'shared_tokens'; -- { serverError INDEX_NOT_USED }

SYSTEM START MERGES json_path_values_special_values;
OPTIMIZE TABLE json_path_values_special_values FINAL;

SELECT groupArray(id) FROM json_path_values_special_values WHERE has(data.tags, '');
SELECT count() FROM json_path_values_special_values WHERE has(data.tags, '')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_special_values;
