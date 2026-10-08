SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS test_column_function_filter;

CREATE TABLE test_column_function_filter (
    id UInt64,
    name String,
    attrs Map(String, String),
    filter_val UInt64
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 100;

INSERT INTO test_column_function_filter
SELECT
    number,
    toString(number),
    map('key1', toString(number), 'key2', toString(number * 2)),
    number % 100
FROM numbers(10000);

-- Test case 1: ColumnFunction shares ColumnConst columns for ['12'] and ['key']
SELECT count() FROM test_column_function_filter
PREWHERE filter_val BETWEEN 20 AND 80
    AND (
        (multiSearchAny(name, ['123', '456', '789']) > 0)
        OR arrayExists(x -> arrayExists(y -> position(y, x) > 0, mapKeys(attrs)), ['12'])
        OR arrayExists(x -> arrayExists(y -> position(y, x) > 0, mapValues(attrs)), ['12'])
    )
    AND (
        arrayExists(x -> arrayExists(y -> position(y, x) > 0, mapKeys(attrs)), ['key'])
        OR arrayExists(x -> arrayExists(y -> position(y, x) > 0, mapValues(attrs)), ['key'])
    );

-- Test case 2: ColumnFunction shares a ColumnArray column for mapKeys(attrs)
SELECT count() FROM test_column_function_filter
PREWHERE filter_val BETWEEN 20 AND 80
    AND (
        (multiSearchAny(name, ['123', '456', '789']) > 0)
        OR arrayExists(x -> arrayExists(y -> position(y, x) > 0, mapKeys(attrs)), ['12'])
    )
    AND arrayExists(x -> arrayExists(y -> position(y, x) > 0, mapKeys(attrs)), ['key']);