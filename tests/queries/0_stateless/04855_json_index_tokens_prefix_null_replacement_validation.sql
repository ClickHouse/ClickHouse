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
        like_pattern Nullable(String),
        ilike_pattern Nullable(String),
        start_needle Nullable(String),
        end_needle Nullable(String),
        regexp Nullable(String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_index_tokens_prefix_null_replacement VALUES
    (1, '{"prefix":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","like_pattern":"a%","ilike_pattern":"a%","start_needle":"abc","end_needle":"def","regexp":"^a"}'),
    (2, '{"prefix":"other","like_pattern":null,"ilike_pattern":null,"start_needle":null,"end_needle":null,"regexp":null}');

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE startsWith(data.prefix, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE data.prefix LIKE 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx%'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE 'abc' LIKE ifNull(data.like_pattern, '');

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE 'ABC' ILIKE ifNull(data.ilike_pattern, '');

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE startsWith('abcdef', ifNull(data.start_needle, 'zzz'));

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE endsWith('abcdef', ifNull(data.end_needle, 'zzz'));

SELECT arraySort(groupArray(id)) FROM json_index_tokens_prefix_null_replacement
WHERE match('abc', ifNull(data.regexp, '^z'));

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
