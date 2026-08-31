SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_path_values_in_subquery;
CREATE TABLE json_path_values_in_subquery
(
    id UInt64,
    data JSON(s String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_path_values_in_subquery;
INSERT INTO json_path_values_in_subquery VALUES (1, '{"s":"one"}');
ALTER TABLE json_path_values_in_subquery
    ADD INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1;
INSERT INTO json_path_values_in_subquery VALUES (2, '{"s":"two"}');

SELECT arraySort(groupArray(id))
FROM json_path_values_in_subquery
WHERE data.s IN (SELECT arrayJoin(['one', 'two']))
SETTINGS force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_in_subquery;
