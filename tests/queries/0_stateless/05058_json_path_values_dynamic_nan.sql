SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET dynamic_throw_on_type_mismatch = 0;
SET input_format_json_try_infer_numbers_from_strings = 1;

DROP TABLE IF EXISTS json_path_values_dynamic_nan;
CREATE TABLE json_path_values_dynamic_nan
(
    id UInt64,
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_dynamic_nan VALUES
    (1, '{"x":"nan"}'),
    (2, '{"x":1.0}');

SELECT count() FROM json_path_values_dynamic_nan
WHERE data.x = nan
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT count() FROM json_path_values_dynamic_nan
WHERE data.x = nan;

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_path_values_dynamic_nan
    WHERE data.x = nan
)
WHERE position(explain, '__text_index') > 0;

SELECT count() FROM json_path_values_dynamic_nan
WHERE data.x = 'nan'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT count() FROM json_path_values_dynamic_nan
WHERE data.x = 'nan';

DROP TABLE json_path_values_dynamic_nan;
