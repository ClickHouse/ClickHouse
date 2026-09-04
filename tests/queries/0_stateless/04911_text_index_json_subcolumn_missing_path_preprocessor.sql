SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS test_json_subcolumn_missing_path_preprocessor;

CREATE TABLE test_json_subcolumn_missing_path_preprocessor
(
    id UInt32,
    data JSON(max_dynamic_paths = 16)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_json_subcolumn_missing_path_preprocessor VALUES
    (1, '{"other":"present"}'),
    (2, '{"target":"hello"}'),
    (3, '{"target":""}');

ALTER TABLE test_json_subcolumn_missing_path_preprocessor
    ADD INDEX idx_json JSONAllValues(data) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = if(empty(JSONAllValues(data)), 'missing', JSONAllValues(data)));

SELECT 'unsafe missing-path predicate before materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_preprocessor
    WHERE hasAnyTokens(data.target::String, '')
    ORDER BY id
);

SELECT 'safe predicate before materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_preprocessor
    WHERE hasAnyTokens(data.target::String, 'hello')
    ORDER BY id
);

ALTER TABLE test_json_subcolumn_missing_path_preprocessor
    MATERIALIZE INDEX idx_json SETTINGS mutations_sync = 2;

SELECT 'unsafe missing-path predicate after materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_preprocessor
    WHERE hasAnyTokens(data.target::String, '')
    ORDER BY id
);

SELECT 'unsafe missing-path predicate does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_missing_path_preprocessor
    WHERE hasAnyTokens(data.target::String, '')
);

SELECT 'safe predicate after materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_preprocessor
    WHERE hasAnyTokens(data.target::String, 'hello')
    ORDER BY id
);

SELECT 'safe predicate selects JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_missing_path_preprocessor
    WHERE hasAnyTokens(data.target::String, 'hello')
);

DROP TABLE test_json_subcolumn_missing_path_preprocessor;
