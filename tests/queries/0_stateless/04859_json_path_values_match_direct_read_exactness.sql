SET enable_json_type = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

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
