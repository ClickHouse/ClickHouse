SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_index_tokens_in;
CREATE TABLE json_index_tokens_in
(
    id UInt64,
    data JSON(lc LowCardinality(String), s String),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_index_tokens_in VALUES
    (1, '{"lc":"alpha","s":"alpha"}'),
    (2, '{"lc":"beta","s":"beta"}'),
    (3, '{"lc":"gamma","s":"gamma"}'),
    (4, '{"lc":"beta","s":"beta"}');

SELECT groupArray(id) FROM json_index_tokens_in WHERE data.lc IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT groupArray(id) FROM json_index_tokens_in WHERE data.lc IN ['beta', 'gamma']
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT groupArray(id) FROM json_index_tokens_in WHERE data.lc IN ('missing', 'alpha')
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT groupArray(id) FROM json_index_tokens_in WHERE data.lc IN ('missing')
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT groupArray(id) FROM json_index_tokens_in WHERE data.lc IN ('alpha', 'gamma')
SETTINGS query_plan_direct_read_from_text_index = 0, force_data_skipping_indices = 'json_tokens';
SELECT groupArray(id) FROM json_index_tokens_in WHERE data.s IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT count() > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_in WHERE data.lc IN ('alpha', 'gamma')
    SETTINGS force_data_skipping_indices = 'json_tokens'
)
WHERE explain LIKE '%mode: Any%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_in WHERE data.lc IN ('alpha', 'gamma')
    SETTINGS force_data_skipping_indices = 'json_tokens'
)
WHERE explain LIKE '%LowCardinality(UInt8)%';

SELECT toTypeName(data.lc IN ('alpha', 'gamma')), groupArray(id)
FROM json_index_tokens_in
WHERE data.lc IN ('alpha', 'gamma')
GROUP BY 1
SETTINGS force_data_skipping_indices = 'json_tokens';

DROP TABLE json_index_tokens_in;
