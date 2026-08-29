SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT 'multiSearchAny empty needle ground truth';
SELECT multiSearchAny('', ['']), multiSearchAny('x', ['']);

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

DROP TABLE IF EXISTS json_index_tokens_long_value;
CREATE TABLE json_index_tokens_long_value
(
    id UInt64,
    data JSON(url String, sparse_a Nullable(String), sparse_b Nullable(String), sparse_c Nullable(String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_index_tokens_long_value VALUES
    (1, concat('{"url":"https://posthog.com/', repeat('a', 100), 'x","sparse_a":"one"}')),
    (2, concat('{"url":"https://posthog.com/', repeat('a', 100), 'y","sparse_b":"two"}')),
    (3, concat('{"url":"HTTPS://POSTHOG.COM/', repeat('A', 100), 'Z","sparse_c":"three"}')),
    (4, '{"url":"https://example.com/other"}');

SELECT 'long equality';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_long_value
WHERE data.url = concat('https://posthog.com/', repeat('a', 100), 'x')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'exact bounded prefixes';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_long_value
WHERE startsWith(data.url, 'https://posthog.com/')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_long_value
WHERE data.url LIKE 'https://posthog.com/%'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_long_value
WHERE data.url ILIKE 'https://posthog.com/%'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'validated prefixes beyond retained bytes';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_long_value
WHERE startsWith(data.url, concat('https://posthog.com/', repeat('a', 80)))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_long_value
WHERE data.url ILIKE concat('https://posthog.com/', repeat('a', 80), '%')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'case-insensitive prefix direct read';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_long_value
    WHERE data.url ILIKE 'https://posthog.com/%'
)
WHERE position(explain, '__text_index') > 0;

SELECT 'sparse typed paths';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_long_value WHERE data.sparse_b = 'two'
SETTINGS force_data_skipping_indices = 'tokens';

CHECK TABLE json_index_tokens_long_value SETTINGS check_query_single_value_result = 1;
DROP TABLE json_index_tokens_long_value;

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
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS json_index_tokens_positions;

DROP TABLE IF EXISTS json_path_values_match_exactness;
CREATE TABLE json_path_values_match_exactness
(
    id UInt64,
    data JSON(url String),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(48)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_match_exactness VALUES
    (1, '{"url":"https://posthog.com/project/123/web"}'),
    (2, '{"url":"https://posthog.com/project/not-a-number/web"}');

SELECT 'direct read with hint';
SELECT groupArray(data.url)
FROM json_path_values_match_exactness
WHERE match(data.url, '^https://posthog[.]com/project/[0-9]+/web$')
SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1;

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM json_path_values_match_exactness
    WHERE match(data.url, '^https://posthog[.]com/project/[0-9]+/web$')
    SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1
)
WHERE position(explain, '__text_index') > 0;

SELECT 'direct read without hint';
SELECT groupArray(data.url)
FROM json_path_values_match_exactness
WHERE match(data.url, '^https://posthog[.]com/project/[0-9]+/web$')
SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 0;

SELECT 'direct read disabled';
SELECT groupArray(data.url)
FROM json_path_values_match_exactness
WHERE match(data.url, '^https://posthog[.]com/project/[0-9]+/web$')
SETTINGS query_plan_direct_read_from_text_index = 0, query_plan_text_index_add_hint = 1;

DROP TABLE json_path_values_match_exactness;

DROP TABLE IF EXISTS json_path_values_pattern_correctness;
CREATE TABLE json_path_values_pattern_correctness
(
    id UInt64,
    data JSON(
        value String,
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

INSERT INTO json_path_values_pattern_correctness VALUES
    (1, '{"value":"abc-value","like_pattern":"a%","ilike_pattern":"a%","start_needle":"abc","end_needle":"def","regexp":"^a"}'),
    (2, '{"value":"other","like_pattern":null,"ilike_pattern":null,"start_needle":null,"end_needle":null,"regexp":null}');

SELECT 'right-side wrappers';
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE 'abc' LIKE ifNull(data.like_pattern, '');
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE 'ABC' ILIKE ifNull(data.ilike_pattern, '');
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE startsWith('abcdef', ifNull(data.start_needle, 'zzz'));
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE endsWith('abcdef', ifNull(data.end_needle, 'zzz'));
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE match('abc', ifNull(data.regexp, '^z'));

SELECT 'commutative equality';
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE 'abc-value' = ifNull(data.value, '')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'dictionary scan setting';
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'abc')
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'abc')
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0,
    text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE data.value LIKE '%value%'
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'minimum pattern length';
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'ab')
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE endsWith(data.value, 'ue')
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE data.value LIKE 'ab%'
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'abc')
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_pattern_correctness;
SET use_skip_indexes = 1;
SET text_index_like_min_pattern_length = 1;

DROP TABLE IF EXISTS json_pv_fixed_string_patterns;
CREATE TABLE json_pv_fixed_string_patterns
(
    id UInt64,
    json JSON(k FixedString(3)),
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_patterns;
INSERT INTO json_pv_fixed_string_patterns VALUES
    (1, '{"k":"a"}'),
    (2, '{"k":"ab"}'),
    (3, '{"k":"b"}');

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE startsWith(json.k, 'a')
SETTINGS force_data_skipping_indices = 'idx';

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE endsWith(json.k, 'a')
SETTINGS force_data_skipping_indices = 'idx';

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE json.k LIKE 'a%'
SETTINGS force_data_skipping_indices = 'idx';

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE json.k ILIKE 'a%'
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_pv_fixed_string_patterns;
