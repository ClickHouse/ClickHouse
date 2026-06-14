-- Tags: no-random-settings

SET enable_analyzer = 1;

-- https://github.com/ClickHouse/ClickHouse/issues/94304
-- Lambda expressions in ALIAS columns should work correctly with `merge()` table function.

DROP TABLE IF EXISTS test_lambda_a;
DROP TABLE IF EXISTS test_lambda_b;

CREATE TABLE test_lambda_a
(
    id UInt64,
    data String,
    mapped Array(UInt64) ALIAS arrayMap(x -> length(x), splitByChar(',', data))
)
ENGINE = MergeTree() ORDER BY id;

CREATE TABLE test_lambda_b
(
    id UInt64,
    data String,
    mapped Array(UInt64) ALIAS arrayMap(x -> length(x) * 2, splitByChar(',', data))
)
ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_lambda_a VALUES (1, 'abc,de,f');
INSERT INTO test_lambda_b VALUES (2, 'xyz,wq');

SELECT id, mapped FROM merge(currentDatabase(), '^test_lambda_') ORDER BY id;

DROP TABLE test_lambda_a;
DROP TABLE test_lambda_b;

-- Test with multiple lambda arguments
CREATE TABLE test_lambda_a
(
    id UInt64,
    keys Array(String),
    vals Array(UInt64),
    result Array(String) ALIAS arrayMap((k, v) -> concat(k, ':', toString(v)), keys, vals)
)
ENGINE = MergeTree() ORDER BY id;

CREATE TABLE test_lambda_b
(
    id UInt64,
    keys Array(String),
    vals Array(UInt64),
    result Array(String) ALIAS arrayMap((k, v) -> concat(toString(v), '=', k), keys, vals)
)
ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_lambda_a VALUES (1, ['a', 'b'], [1, 2]);
INSERT INTO test_lambda_b VALUES (2, ['c', 'd'], [3, 4]);

SELECT id, result FROM merge(currentDatabase(), '^test_lambda_') ORDER BY id;

DROP TABLE test_lambda_a;
DROP TABLE test_lambda_b;
