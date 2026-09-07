SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS test_json_subcolumn_missing_path_safe_predicates;

CREATE TABLE test_json_subcolumn_missing_path_safe_predicates
(
    id UInt32,
    data JSON(max_dynamic_paths = 16)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_json_subcolumn_missing_path_safe_predicates VALUES
    (1, '{"other":"present"}'),
    (2, '{"target":"missing"}'),
    (3, '{"target":"prefix missing suffix"}'),
    (4, '{"target":""}');

ALTER TABLE test_json_subcolumn_missing_path_safe_predicates
    ADD INDEX idx_json JSONAllValues(data) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = if(empty(JSONAllValues(data)), 'missing', JSONAllValues(data)));

ALTER TABLE test_json_subcolumn_missing_path_safe_predicates
    MATERIALIZE INDEX idx_json SETTINGS mutations_sync = 2;

SELECT 'equals keeps the JSONAllValues index hint';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_safe_predicates
    WHERE data.target::String = 'missing'
    ORDER BY id
);

SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_missing_path_safe_predicates
    WHERE data.target::String = 'missing'
);

SELECT 'LIKE keeps the JSONAllValues index hint';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_safe_predicates
    WHERE data.target::String LIKE '%missing%'
    ORDER BY id
);

SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_missing_path_safe_predicates
    WHERE data.target::String LIKE '%missing%'
);

SELECT 'match keeps the JSONAllValues index hint';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_missing_path_safe_predicates
    WHERE match(data.target::String, ' missing ')
    ORDER BY id
);

SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_missing_path_safe_predicates
    WHERE match(data.target::String, ' missing ')
);

DROP TABLE test_json_subcolumn_missing_path_safe_predicates;
