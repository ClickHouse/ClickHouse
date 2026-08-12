SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_index_tokens_dynamic_cast;
CREATE TABLE json_index_tokens_dynamic_cast
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(32)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_index_tokens_dynamic_cast VALUES
    (1, '{"value":42}'),
    (2, '{"value":"42"}'),
    (3, '{"value":43}'),
    (4, '{"value":[1,2,3,4,5]}'),
    (5, '{}');

SELECT groupArray(id) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '42'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT groupArray(id) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '[1,2,3,4,5]'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT groupArray(id) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '__missing__'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT groupArray(id) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '42'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '42'
)
WHERE position(explain, '__text_index') > 0;

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_dynamic_cast WHERE toString(data.value) = '42'
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE json_index_tokens_dynamic_cast;
