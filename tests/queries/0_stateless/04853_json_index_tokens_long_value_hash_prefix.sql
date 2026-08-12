SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

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
SELECT groupArray(id) FROM json_index_tokens_long_value
WHERE data.url = concat('https://posthog.com/', repeat('a', 100), 'x')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'exact bounded prefixes';
SELECT groupArray(id) FROM json_index_tokens_long_value
WHERE startsWith(data.url, 'https://posthog.com/')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_index_tokens_long_value
WHERE data.url LIKE 'https://posthog.com/%'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_index_tokens_long_value
WHERE data.url ILIKE 'https://posthog.com/%'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'validated prefixes beyond retained bytes';
SELECT groupArray(id) FROM json_index_tokens_long_value
WHERE startsWith(data.url, concat('https://posthog.com/', repeat('a', 80)))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_index_tokens_long_value
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
SELECT groupArray(id) FROM json_index_tokens_long_value WHERE data.sparse_b = 'two'
SETTINGS force_data_skipping_indices = 'tokens';

CHECK TABLE json_index_tokens_long_value SETTINGS check_query_single_value_result = 1;
DROP TABLE json_index_tokens_long_value;
