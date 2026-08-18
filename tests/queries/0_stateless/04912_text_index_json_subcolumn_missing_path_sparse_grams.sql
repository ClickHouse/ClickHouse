SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS test_json_subcolumn_missing_path_sparse_grams;

CREATE TABLE test_json_subcolumn_missing_path_sparse_grams
(
    id UInt32,
    data JSON(max_dynamic_paths = 16)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_json_subcolumn_missing_path_sparse_grams VALUES
    (1, '{"other":"zzz"}'),
    (2, '{"target":"cde"}'),
    (3, '{"target":"xyz"}');

ALTER TABLE test_json_subcolumn_missing_path_sparse_grams
    ADD INDEX idx_json JSONAllValues(data) TYPE text(
        tokenizer = sparseGrams(3, 8),
        preprocessor = if(empty(JSONAllValues(data)), 'abcdef', JSONAllValues(data)));

SELECT 'unsafe covered gram before materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_sparse_grams
    WHERE hasAnyTokens(data.target::String, 'cde')
    ORDER BY id
);

SELECT 'unsafe covered gram does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_missing_path_sparse_grams
    WHERE hasAnyTokens(data.target::String, 'cde')
);

SELECT 'safe gram before materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_sparse_grams
    WHERE hasAnyTokens(data.target::String, 'xyz')
    ORDER BY id
);

ALTER TABLE test_json_subcolumn_missing_path_sparse_grams
    MATERIALIZE INDEX idx_json SETTINGS mutations_sync = 2;

SELECT 'unsafe covered gram after materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_sparse_grams
    WHERE hasAnyTokens(data.target::String, 'cde')
    ORDER BY id
);

SELECT 'safe gram after materialization';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_sparse_grams
    WHERE hasAnyTokens(data.target::String, 'xyz')
    ORDER BY id
);

SELECT 'safe gram selects JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_missing_path_sparse_grams
    WHERE hasAnyTokens(data.target::String, 'xyz')
);

DROP TABLE test_json_subcolumn_missing_path_sparse_grams;
