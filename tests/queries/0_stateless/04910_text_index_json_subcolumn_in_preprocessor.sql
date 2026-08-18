SET enable_full_text_index = 1;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS test_json_subcolumn_in_preprocessor;

CREATE TABLE test_json_subcolumn_in_preprocessor
(
    id UInt32,
    data JSON(max_dynamic_paths = 16),
    INDEX idx_json JSONAllValues(data) TYPE text(
        tokenizer = ngrams(3),
        preprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_json_subcolumn_in_preprocessor VALUES
    (1, '{"value":"abc"}'),
    (2, '{"value":"ABC"}');

SELECT 'FixedString IN keeps query semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE data.value::FixedString(5) IN ('abc')
    ORDER BY id
);

SELECT 'FixedString IN does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE data.value::FixedString(5) IN ('abc')
);

SELECT 'FixedString GLOBAL IN keeps query semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE data.value::FixedString(5) GLOBAL IN ('abc')
    ORDER BY id
);

SELECT 'FixedString GLOBAL IN does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE data.value::FixedString(5) GLOBAL IN ('abc')
);

SELECT 'FixedString tuple IN keeps query semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE (data.value::FixedString(5), id) IN (('abc', 1))
    ORDER BY id
);

SELECT 'FixedString tuple IN does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE (data.value::FixedString(5), id) IN (('abc', 1))
);

SELECT 'String IN applies preprocessor and can use JSONAllValues index';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE data.value::String IN ('ABC')
    ORDER BY id
);

SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_in_preprocessor
    WHERE data.value::String IN ('ABC')
);

DROP TABLE test_json_subcolumn_in_preprocessor;
