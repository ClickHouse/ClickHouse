SET enable_json_type = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_path_values_fixed_string_subquery_in;
CREATE TABLE json_path_values_fixed_string_subquery_in
(
    id UInt64,
    data JSON(k FixedString(3)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_fixed_string_subquery_in VALUES
    (1, '{"k":"a"}'),
    (2, '{"k":"b"}');

SELECT count() FROM json_path_values_fixed_string_subquery_in WHERE data.k IN (SELECT 'a');
SELECT count() FROM json_path_values_fixed_string_subquery_in WHERE data.k IN (SELECT 'a')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_path_values_fixed_string_subquery_in WHERE data.k GLOBAL IN (SELECT 'a');
SELECT count() FROM json_path_values_fixed_string_subquery_in WHERE data.k GLOBAL IN (SELECT 'a')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_fixed_string_subquery_in;
