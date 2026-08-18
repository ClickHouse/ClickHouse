SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS test_json_subcolumn_preprocessor;

CREATE TABLE test_json_subcolumn_preprocessor
(
    id UInt32,
    data JSON(max_dynamic_paths = 16),
    INDEX idx_json JSONAllValues(data) TYPE text(
        tokenizer = 'asciiCJK',
        preprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_json_subcolumn_preprocessor VALUES
    (1, '{"dynamic_text":"MiXeD_Case token"}'),
    (2, '{"dynamic_text":"mixed_case token"}'),
    (3, '{"dynamic_text":"mixed case token"}'),
    (4, '{"dynamic_text":"unrelated token"}');

SELECT 'dynamic paths';
SELECT arraySort(arrayDistinct(arrayFlatten(groupArray(JSONDynamicPaths(data)))))
FROM test_json_subcolumn_preprocessor;

SELECT 'JSONAllValues tokenizer';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(JSONAllValues(data), 'mixed')
    ORDER BY id
);

SELECT 'JSONAllValues preprocessor';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(JSONAllValues(data), 'MIXED_CASE')
    ORDER BY id
);

SELECT 'JSON subcolumn tokenizer';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(data.dynamic_text::String, 'mixed')
    ORDER BY id
);

SELECT 'JSON subcolumn preprocessor with lowercase needle';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(data.dynamic_text::String, 'mixed_case')
    ORDER BY id
);

SELECT 'JSON subcolumn preprocessor with uppercase needle';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(data.dynamic_text::String, 'MIXED_CASE')
    ORDER BY id
);

SELECT 'JSON subcolumn preprocessor without hint direct read';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(data.dynamic_text::String, 'MIXED_CASE')
    ORDER BY id
)
SETTINGS query_plan_direct_read_from_text_index = 0,
         query_plan_text_index_add_hint = 0;

SELECT 'JSONAllValues index selected for subcolumn';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(data.dynamic_text::String, 'mixed')
);

SELECT 'text-index virtual column created for subcolumn';
SELECT countIf(position(explain, '__text_index_idx_json_hasAnyTokens_') > 0) > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(data.dynamic_text::String, 'mixed')
);

SELECT 'unmatched expression keeps default tokenizer semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(lower(data.dynamic_text::String), 'mixed')
    ORDER BY id
);

SELECT 'unmatched expression does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(lower(data.dynamic_text::String), 'mixed')
);

SELECT 'unmatched expression does not create text-index virtual column';
SELECT countIf(position(explain, '__text_index_idx_json_hasAnyTokens_') > 0) > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM test_json_subcolumn_preprocessor
    WHERE hasAnyTokens(lower(data.dynamic_text::String), 'mixed')
);

DROP TABLE test_json_subcolumn_preprocessor;

DROP TABLE IF EXISTS test_json_subcolumn_prepostprocessor;

CREATE TABLE test_json_subcolumn_prepostprocessor
(
    id UInt32,
    data JSON(max_dynamic_paths = 16),
    INDEX idx_json JSONAllValues(data) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = lower(JSONAllValues(data)),
        postprocessor = replaceRegexpAll(JSONAllValues(data), '[aeiou]', ''))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_json_subcolumn_prepostprocessor VALUES
    (1, '{"first":"Hello","second":"World","array_text":["MiXeD"]}'),
    (2, '{"first":"Other","second":"Value","array_text":["other"]}'),
    (3, '{}');

SELECT 'combined preprocessor and postprocessor';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasAnyTokens(data.first::String, 'HELLO')
    ORDER BY id
);

SELECT 'combined preprocessor and postprocessor without hint direct read';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasAnyTokens(data.first::String, 'HELLO')
    ORDER BY id
)
SETTINGS query_plan_direct_read_from_text_index = 0,
         query_plan_text_index_add_hint = 0;

SELECT 'combined preprocessor and postprocessor with hasToken';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasToken(data.first::String, 'HELLO')
    ORDER BY id
);

SELECT 'combined preprocessor and postprocessor with hasPhrase';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasPhrase(data.first::String, 'HELLO')
    ORDER BY id
);

SELECT 'combined preprocessor and postprocessor on multiple paths';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasAnyTokens(data.first::String, 'HELLO')
      AND hasAnyTokens(data.second::String, 'WORLD')
    ORDER BY id
);

SELECT 'array subcolumn keeps default semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasAnyTokens(data.array_text::Array(String), 'mixed')
    ORDER BY id
);

SELECT 'array subcolumn does not select JSONAllValues index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasAnyTokens(data.array_text::Array(String), 'mixed')
);

SELECT 'array subcolumn does not create text-index virtual column';
SELECT countIf(position(explain, '__text_index_idx_json_hasAnyTokens_') > 0) > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM test_json_subcolumn_prepostprocessor
    WHERE hasAnyTokens(data.array_text::Array(String), 'mixed')
);

DROP TABLE test_json_subcolumn_prepostprocessor;
