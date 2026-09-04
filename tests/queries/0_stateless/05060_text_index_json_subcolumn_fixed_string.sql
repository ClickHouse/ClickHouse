SET enable_full_text_index = 1;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS test_text_index_json_subcolumn_fixed_string;

CREATE TABLE test_text_index_json_subcolumn_fixed_string
(
    id UInt32,
    data JSON(max_dynamic_paths = 16),
    INDEX idx JSONAllValues(data) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_text_index_json_subcolumn_fixed_string VALUES
    (1, '{"value":"abc"}'),
    (2, '{"value":"other"}');

SELECT 'String subcolumn can use JSONAllValues index';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_text_index_json_subcolumn_fixed_string
    WHERE hasAnyTokens(data.value::String, 'abc')
    ORDER BY id
);

SELECT countIf(position(explain, 'Name: idx') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_text_index_json_subcolumn_fixed_string
    WHERE hasAnyTokens(data.value::String, 'abc')
);

SELECT 'FixedString subcolumn keeps query semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_text_index_json_subcolumn_fixed_string
    WHERE hasAnyTokens(data.value::FixedString(5), concat('c', char(0), char(0)), 'ngrams(3)')
    ORDER BY id
);

SELECT 'FixedString subcolumn does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_text_index_json_subcolumn_fixed_string
    WHERE hasAnyTokens(data.value::FixedString(5), concat('c', char(0), char(0)), 'ngrams(3)')
);

DROP TABLE test_text_index_json_subcolumn_fixed_string;
