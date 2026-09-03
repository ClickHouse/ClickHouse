SET enable_full_text_index = 1;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS test_text_index_in_fixed_string_set;

CREATE TABLE test_text_index_in_fixed_string_set
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(3), preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_text_index_in_fixed_string_set VALUES
    (1, 'abc'),
    (2, 'ABC'),
    (3, 'other');

SELECT 'String IN applies preprocessor and can use text index';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE s IN ('ABC')
    ORDER BY id
);

SELECT countIf(position(explain, 'Name: idx') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE s IN ('ABC')
);

SELECT 'String IN with FixedString set keeps query semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE s IN (SELECT toFixedString('ABC', 5))
    ORDER BY id
);

SELECT 'String IN with FixedString set does not select text index';
SELECT countIf(position(explain, 'Name: idx') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE s IN (SELECT toFixedString('ABC', 5))
);

SELECT 'String GLOBAL IN with FixedString set keeps query semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE s GLOBAL IN (SELECT toFixedString('ABC', 5))
    ORDER BY id
);

SELECT 'String GLOBAL IN with FixedString set does not select text index';
SELECT countIf(position(explain, 'Name: idx') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE s GLOBAL IN (SELECT toFixedString('ABC', 5))
);

SELECT 'String tuple IN with FixedString set keeps query semantics';
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE (s, id) IN (SELECT toFixedString('ABC', 5), toUInt32(2))
    ORDER BY id
);

SELECT 'String tuple IN with FixedString set does not select text index';
SELECT countIf(position(explain, 'Name: idx') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM test_text_index_in_fixed_string_set
    WHERE (s, id) IN (SELECT toFixedString('ABC', 5), toUInt32(2))
);

DROP TABLE test_text_index_in_fixed_string_set;
