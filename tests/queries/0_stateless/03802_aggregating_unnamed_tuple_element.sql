SET optimize_throw_if_noop = 1;

-- Test 1: Unnamed Tuple (positional access) with aggregation
SELECT '=== Test 1: Unnamed Tuple (positional access) ===';
DROP TABLE IF EXISTS test_unnamed_tuple_agg;

CREATE TABLE test_unnamed_tuple_agg (
    metrics Tuple(
        UInt64,
        SimpleAggregateFunction(sum, Double),
        SimpleAggregateFunction(max, UInt64),
        SimpleAggregateFunction(anyLast, String),
        AggregateFunction(uniq, String),
        AggregateFunction(count)
    )
) ENGINE = AggregatingMergeTree() ORDER BY metrics.1
SETTINGS allow_tuple_element_aggregation = 1;

-- First insert with unnamed tuple
INSERT INTO test_unnamed_tuple_agg
SELECT tuple(
    number, 
    number * 1.5, 
    number * 10, 
    concat('str_', toString(number)),
    uniqState(concat('val_', toString(number))),
    countState()
) 
FROM numbers(10)
GROUP BY number;

-- Second insert to test merge behavior with same keys
INSERT INTO test_unnamed_tuple_agg 
SELECT tuple(
    number, 
    number * 2.5, 
    number * 20, 
    concat('new_', toString(number)),
    uniqState(concat('new_val_', toString(number))),
    countState()
) 
FROM numbers(10)
GROUP BY number;

-- Test with FINAL to verify immediate aggregation using positional access
SELECT 'Unnamed Tuple test - with FINAL (after two inserts):';
SELECT 
    metrics.1 AS id,
    metrics.2 AS sum_val,
    metrics.3 AS max_val,
    metrics.4 AS any_last_val,
    uniqMerge(metrics.5) AS unique_count,
    countMerge(metrics.6) AS total_count
FROM test_unnamed_tuple_agg FINAL 
GROUP BY metrics.1, metrics.2, metrics.3, metrics.4
ORDER BY metrics.1;

-- Test OPTIMIZE to verify on-disk aggregation
OPTIMIZE TABLE test_unnamed_tuple_agg FINAL;

SELECT 'Unnamed Tuple test - after OPTIMIZE:';
SELECT 
    metrics.1 AS id,
    metrics.2 AS sum_val,
    metrics.3 AS max_val,
    metrics.4 AS any_last_val,
    uniqMerge(metrics.5) AS unique_count,
    countMerge(metrics.6) AS total_count
FROM test_unnamed_tuple_agg 
GROUP BY metrics.1, metrics.2, metrics.3, metrics.4
ORDER BY metrics.1;

DROP TABLE test_unnamed_tuple_agg;

-- Test 2: Nested unnamed tuple
SELECT '=== Test 2: Nested Unnamed Tuple ===';
DROP TABLE IF EXISTS test_nested_unnamed_tuple;

CREATE TABLE test_nested_unnamed_tuple (
    data Tuple(
        UInt32,
        Tuple(
            SimpleAggregateFunction(sum, UInt64),
            SimpleAggregateFunction(max, UInt64),
            Tuple(
                SimpleAggregateFunction(min, UInt64),
                AggregateFunction(avg, Float64)
            )
        )
    )
) ENGINE = AggregatingMergeTree() ORDER BY data.1
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert nested unnamed tuple data
INSERT INTO test_nested_unnamed_tuple
SELECT tuple(
    number % 3,
    tuple(
        number * 100,
        number * 10,
        tuple(
            number,
            avgState(toFloat64(number))
        )
    )
)
FROM numbers(10)
GROUP BY number;

-- Insert more data with same keys
INSERT INTO test_nested_unnamed_tuple
SELECT tuple(
    number % 3,
    tuple(
        number * 50,
        number * 20,
        tuple(
            number + 5,
            avgState(toFloat64(number + 10))
        )
    )
)
FROM numbers(10)
GROUP BY number;

-- Test nested unnamed tuple aggregation
SELECT 'Nested Unnamed Tuple test - with FINAL:';
SELECT 
    data.1 AS key,
    data.2.1 AS sum_val,
    data.2.2 AS max_val,
    data.2.3.1 AS min_val,
    avgMerge(data.2.3.2) AS avg_val
FROM test_nested_unnamed_tuple FINAL
GROUP BY data.1, data.2.1, data.2.2, data.2.3.1
ORDER BY data.1;

OPTIMIZE TABLE test_nested_unnamed_tuple FINAL;

SELECT 'Nested Unnamed Tuple test - after OPTIMIZE:';
SELECT 
    data.1 AS key,
    data.2.1 AS sum_val,
    data.2.2 AS max_val,
    data.2.3.1 AS min_val,
    avgMerge(data.2.3.2) AS avg_val
FROM test_nested_unnamed_tuple
GROUP BY data.1, data.2.1, data.2.2, data.2.3.1
ORDER BY data.1;

DROP TABLE test_nested_unnamed_tuple;
