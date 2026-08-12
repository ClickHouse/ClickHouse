SET enable_json_type = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS json_path_values_nested_subcolumns;
CREATE TABLE json_path_values_nested_subcolumns
(
    id UInt64,
    data JSON(
        str String,
        arr Array(String),
        n Nullable(Int64),
        tuple Tuple(value String),
        map Map(String, String)),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_nested_subcolumns VALUES
    (1, '{"str":"a","arr":["x","y"],"n":1,"tuple":{"value":"x"},"map":{"k":"v"}}'),
    (2, '{"str":"","arr":[],"n":null,"tuple":{"value":"y"},"map":{}}'),
    (3, '{"str":"c","arr":["z"],"tuple":{"value":"x"},"map":{"q":"w"}}');

SELECT 'optimized subcolumns';
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE data.str = '';
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE empty(data.str);
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE length(data.arr) = 2;
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE empty(data.arr);

SELECT 'nested typed subcolumns';
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE data.tuple.value = 'x';
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE has(data.map.keys, 'k');
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE data.n.null = 1;

SELECT count() FROM json_path_values_nested_subcolumns WHERE data.str = ''
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_nested_subcolumns WHERE length(data.arr) = 2
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_nested_subcolumns WHERE data.tuple.value = 'x'
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE has(data.map.keys, 'k')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT count() FROM json_path_values_nested_subcolumns WHERE data.n.null = 1
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'direct typed path';
SELECT groupArray(id) FROM json_path_values_nested_subcolumns WHERE data.str = ''
SETTINGS
    optimize_empty_string_comparisons = 0,
    optimize_functions_to_subcolumns = 0;
SELECT count() FROM json_path_values_nested_subcolumns WHERE data.str = ''
SETTINGS
    optimize_empty_string_comparisons = 0,
    optimize_functions_to_subcolumns = 0,
    force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_nested_subcolumns;
