SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_index_tokens_nullable_in;
CREATE TABLE json_index_tokens_nullable_in
(
    id UInt64,
    data JSON(s Nullable(String), fixed Nullable(FixedString(3))),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_nullable_in;
INSERT INTO json_index_tokens_nullable_in VALUES
    (1, '{"s":"alpha","fixed":"one"}'),
    (2, '{"s":"beta","fixed":"two"}'),
    (3, '{"s":"gamma","fixed":"one"}'),
    (4, '{"s":null,"fixed":null}'),
    (5, '{}');

SET transform_null_in = 1;

SELECT groupArray(id) FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_index_tokens_nullable_in WHERE data.s GLOBAL IN ('beta')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_index_tokens_nullable_in WHERE data.fixed IN ('one')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_index_tokens_nullable_in WHERE (data.s, id) IN (('alpha', 1), ('gamma', 3))
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id) FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', NULL);
SELECT count() = 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', NULL)
)
WHERE explain LIKE '%Name: tokens%';

SET transform_null_in = 0;
SELECT groupArray(id) FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'tokens';

DROP TABLE json_index_tokens_nullable_in;
