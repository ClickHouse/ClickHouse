SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes_on_data_read = 1;

SELECT 'DDL validation';
DROP TABLE IF EXISTS json_index_tokens_invalid_configuration;
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(0))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(1048577))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(63 + 1))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues('64'))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64, 'none'))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'LowCardinality analyzer';
DROP TABLE IF EXISTS json_index_tokens_low_cardinality;
CREATE TABLE json_index_tokens_low_cardinality
(
    id UInt64,
    default_data JSON(s LowCardinality(String)),
    one_arg_data JSON(s LowCardinality(String)),
    INDEX default_tokens default_data TYPE text(tokenizer = jsonPathValues) GRANULARITY 1,
    INDEX one_arg_tokens one_arg_data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_index_tokens_low_cardinality VALUES
    (1, '{"s":""}', '{"s":""}'),
    (2, '{"s":"x"}', '{"s":"x"}');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_low_cardinality WHERE one_arg_data.s = 'x'
SETTINGS force_data_skipping_indices = 'one_arg_tokens';
SELECT count() FROM json_index_tokens_low_cardinality WHERE one_arg_data.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'one_arg_tokens'; -- { serverError INDEX_NOT_USED }
DROP TABLE json_index_tokens_low_cardinality;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS json_index_tokens;
CREATE TABLE json_index_tokens
(
    id UInt64,
    data JSON(email String, url String, other String, count Nullable(UInt64), flag Nullable(Bool)),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO json_index_tokens VALUES
    (1, '{"email":"alice@example.com","url":"https://posthog.com/docs","count":42,"flag":true}'),
    (2, concat('{"email":"Bob@Example.com","url":"https://posthog.com/', repeat('a', 160), '"}')),
    (3, '{"email":null,"url":"http://example.org","other":"alice@example.com"}'),
    (4, '{"email":"carol@example.com"}'),
    (5, concat('{"url":"https://posthog.com/', repeat('a', 50), 'b"}'));

SELECT 'equality direct read off';
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.email = 'alice@example.com'
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'equality complete';
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.email = 'alice@example.com';

SELECT 'equality empty string';
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.email = ''
SETTINGS optimize_empty_string_comparisons = 0;

SELECT 'empty string predicates';
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.email IN ('', 'alice@example.com')
SETTINGS optimize_empty_string_comparisons = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.email LIKE '%'
SETTINGS text_index_like_min_pattern_length = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.email ILIKE '%'
SETTINGS text_index_like_min_pattern_length = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE match(data.email, '^$')
SETTINGS text_index_like_min_pattern_length = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE multiSearchAny(data.email, [''])
SETTINGS text_index_like_min_pattern_length = 0;

SELECT 'equality typed values';
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.count = 42;
SELECT arraySort(groupArray(id)) FROM json_index_tokens WHERE data.flag = true;

SELECT 'equality truncated';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.url = concat('https://posthog.com/', repeat('a', 160));

SELECT 'in';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.email IN ('alice@example.com', 'carol@example.com');

SELECT 'startsWith bounded prefix';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE startsWith(data.url, 'https://posthog.com/');

SELECT 'like prefix';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.url LIKE 'https://posthog.com/%';

SELECT 'startsWith fallback';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE startsWith(data.url, 'https://posthog.com/')
SETTINGS text_index_like_max_postings_to_read = 0;

SELECT 'startsWith beyond stored prefix';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE startsWith(data.url, concat('https://posthog.com/', repeat('a', 80)));

SELECT 'like';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.email LIKE '%example.com%';

SELECT 'like after stored prefix';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.url LIKE '%b%';

SELECT 'like dictionary fallback';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.email LIKE '%example.com%'
SETTINGS text_index_like_max_postings_to_read = 0;

SELECT 'ilike';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.email ILIKE '%EXAMPLE.COM%';

SELECT 'endsWith';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE endsWith(data.url, 'docs');
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE endsWith(data.url, 'b');

SELECT 'match hint';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE match(data.email, '^alice@.*\\.com$');

SELECT 'multiSearchAny';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE multiSearchAny(data.email, ['alice@', 'carol@']);

SELECT 'path isolation';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.other LIKE '%example.com%';

SELECT 'direct read plan';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens WHERE data.email = 'alice@example.com'
    SETTINGS query_plan_optimize_count_from_text_index = 0
)
WHERE position(explain, '__text_index') > 0;

SELECT 'dictionary scan plan';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens WHERE data.email LIKE '%example.com%'
)
WHERE position(explain, '__text_index') > 0;

SET allow_experimental_text_index_lazy_apply = 1;
SET text_index_posting_list_apply_mode = 'lazy';

SELECT 'lazy patterns';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.email LIKE '%example.com%';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE data.url LIKE '%b%';
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE endsWith(data.url, 'b');
SELECT arraySort(groupArray(id)) FROM json_index_tokens
WHERE multiSearchAny(data.email, ['alice@', 'carol@']);

CHECK TABLE json_index_tokens SETTINGS check_query_single_value_result = 1;

DROP TABLE json_index_tokens;

CREATE TABLE json_index_tokens_bad
(
    data JSON,
    INDEX json_tokens data TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
