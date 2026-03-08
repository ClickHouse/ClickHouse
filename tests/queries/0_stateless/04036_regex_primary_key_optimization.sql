-- Tags: no-fasttest
-- Verify that simple regex patterns with groups and alternation
-- can use the primary key index for range filtering.
-- See https://github.com/ClickHouse/ClickHouse/issues/82159

DROP TABLE IF EXISTS test_regex_pk;

CREATE TABLE test_regex_pk
(
    id String
)
ENGINE = MergeTree()
PRIMARY KEY id
ORDER BY id;

INSERT INTO test_regex_pk
SELECT arrayElement(
    ['vector-abc-001', 'vector-abc-002', 'metrics-def-003', 'metrics-def-004',
     'logs-ghi-005', 'logs-ghi-006', 'traces-jkl-007', 'traces-jkl-008'],
    (number % 8) + 1
)
FROM numbers(80000);

-- Test 1: Simple anchored regex without groups — should use primary key (already works)
-- Expect: Parts = 1, Granules < total
SELECT 'simple anchored';
SELECT count() FROM test_regex_pk WHERE id REGEXP '^vector-abc-001$';

-- Test 2: Anchored regex WITH group — should now use primary key (was broken)
SELECT 'grouped anchored';
SELECT count() FROM test_regex_pk WHERE id REGEXP '^(vector-abc-001)$';

-- Test 3: Alternation with common prefix — should use primary key with prefix range
SELECT 'alternation common prefix';
SELECT count() FROM test_regex_pk WHERE id REGEXP '^(vector-abc-001|vector-abc-002)$';

-- Test 4: Alternation with NO common prefix — cannot use primary key (expected)
SELECT 'alternation no common prefix';
SELECT count() FROM test_regex_pk WHERE id REGEXP '^(vector-abc-001|metrics-def-003)$';

-- Test 5: Verify results are correct
SELECT 'result check';
SELECT count() FROM test_regex_pk WHERE id REGEXP '^(vector-abc-001|vector-abc-002)$';
SELECT count() FROM test_regex_pk WHERE id IN ('vector-abc-001', 'vector-abc-002');

DROP TABLE test_regex_pk;
