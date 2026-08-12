SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT 'multiSearchAny empty needle ground truth';
SELECT multiSearchAny('', ['']), multiSearchAny('x', ['']);

DROP TABLE IF EXISTS json_index_tokens_nonliteral;
CREATE TABLE json_index_tokens_nonliteral
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(63 + 1))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS json_index_tokens_cast_ilike;
CREATE TABLE json_index_tokens_cast_ilike
(
    id UInt64,
    data JSON(email String, Email String, count UInt64),
    dynamic_data JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX dynamic_tokens dynamic_data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_cast_ilike;
INSERT INTO json_index_tokens_cast_ilike VALUES
    (1, '{"email":"Alice@example.com","Email":"other","count":42}', '{"value":42}');
INSERT INTO json_index_tokens_cast_ilike VALUES
    (2, '{"email":"bob@example.com","Email":"ALICE@example.com","count":43}', '{"value":"42"}');
INSERT INTO json_index_tokens_cast_ilike VALUES
    (3, '{"Email":"ALICE@example.com","count":42}', '{"value":42.0}');
INSERT INTO json_index_tokens_cast_ilike VALUES
    (4, '{"Email":"ALICE@example.com","count":44}', '{"value":[42]}');

SELECT 'ILIKE path is case-sensitive';
SELECT arraySort(groupArray(id))
FROM json_index_tokens_cast_ilike
WHERE data.email ILIKE '%ALICE%'
SETTINGS force_data_skipping_indices = 'data_tokens';

SELECT 'typed CAST is not indexed';
SELECT arraySort(groupArray(id))
FROM json_index_tokens_cast_ilike
WHERE CAST(data.count AS String) = '42';
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_cast_ilike WHERE CAST(data.count AS String) = '42'
)
WHERE position(explain, '__text_index') > 0;

SELECT 'Dynamic CAST to String';
SELECT arraySort(groupArray(id))
FROM json_index_tokens_cast_ilike
WHERE CAST(dynamic_data.value AS String) = '42'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id))
FROM json_index_tokens_cast_ilike
WHERE CAST(dynamic_data.value AS String) = '[42]'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_cast_ilike WHERE CAST(dynamic_data.value AS String) = '42'
)
WHERE position(explain, '__text_index') > 0;

SELECT 'Dynamic CAST patterns are not indexed';
SELECT arraySort(groupArray(id))
FROM json_index_tokens_cast_ilike
WHERE CAST(dynamic_data.value AS String) LIKE '%42%'
SETTINGS dynamic_throw_on_type_mismatch = 0, text_index_like_min_pattern_length = 0;
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_cast_ilike WHERE CAST(dynamic_data.value AS String) LIKE '%42%'
    SETTINGS dynamic_throw_on_type_mismatch = 0, text_index_like_min_pattern_length = 0
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE json_index_tokens_cast_ilike;
