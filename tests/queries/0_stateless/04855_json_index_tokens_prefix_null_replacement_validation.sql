SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS json_index_tokens_prefix_null_replacement;
CREATE TABLE json_index_tokens_prefix_null_replacement
(
    id UInt64,
    data JSON(
        prefix String,
        start_needle Nullable(String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_index_tokens_prefix_null_replacement VALUES
    (1, '{"prefix":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","start_needle":"abc"}'),
    (2, '{"prefix":"other","start_needle":null}');

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE startsWith(data.prefix, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE data.prefix LIKE 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx%'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE 'abc' = ifNull(data.start_needle, '');

DROP TABLE json_index_tokens_prefix_null_replacement;

DROP TABLE IF EXISTS json_index_tokens_positions;
CREATE TABLE json_index_tokens_positions
(
    data JSON(value String),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), positions = 1)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS json_index_tokens_positions;
