-- Test that REGEXP with exact-match patterns uses primary key index
-- See https://github.com/ClickHouse/ClickHouse/issues/82159

DROP TABLE IF EXISTS test_regexp_pk;
CREATE TABLE test_regexp_pk (id String) ENGINE = MergeTree() PRIMARY KEY id ORDER BY id;

INSERT INTO test_regexp_pk SELECT arrayElement(
    ['vector-abc-001', 'vector-abc-002', 'metrics-def-003', 'metrics-def-004', 'logs-ghi-005'],
    (number % 5) + 1
) FROM numbers(10000);

-- Without parens: primary key already works
SELECT count() FROM test_regexp_pk WHERE id REGEXP '^vector-abc-001$';

-- Single value in parens: should now use primary key
SELECT count() FROM test_regexp_pk WHERE id REGEXP '^(vector-abc-001)$';

-- Multiple alternatives: should now use primary key
SELECT count() FROM test_regexp_pk WHERE id REGEXP '^(vector-abc-001|metrics-def-003)$';

-- Pattern with actual metacharacters should fall back to prefix extraction
SELECT count() FROM test_regexp_pk WHERE id REGEXP '^vector-abc-.*$';

DROP TABLE test_regexp_pk;
