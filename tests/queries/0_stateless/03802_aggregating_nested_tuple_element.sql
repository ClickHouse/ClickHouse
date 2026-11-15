SET optimize_throw_if_noop = 1;

-- Test 1: Multi-level nested Tuple aggregation
SELECT '=== Test 1: Multi-level nested Tuple aggregation ===';
DROP TABLE IF EXISTS test_nested_tuple_agg;

CREATE TABLE test_nested_tuple_agg (
    id UInt32,
    nested_metrics Tuple(
        level1 Tuple(
            simple_sum SimpleAggregateFunction(sum, UInt64),
            level2 Tuple(
                simple_max SimpleAggregateFunction(max, UInt64),
                level3 Tuple(
                    simple_min SimpleAggregateFunction(min, UInt64),
                    agg_uniq AggregateFunction(uniq, String),
                    level4 Tuple(
                        simple_any_last SimpleAggregateFunction(anyLast, String),
                        agg_count AggregateFunction(count)
                    )
                )
            )
        ),
        parallel_branch Tuple(
            simple_bit_or SimpleAggregateFunction(groupBitOr, UInt32),
            agg_avg AggregateFunction(avg, Float64)
        )
    )
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert first batch of nested data
INSERT INTO test_nested_tuple_agg
SELECT
    1 AS id,
    tuple(
        tuple(
            100,
            tuple(
                50,
                tuple(
                    10,
                    uniqState('value1'),
                    tuple(
                        'first',
                        countState()
                    )
                )
            )
        ),
        tuple(
            1,
            avgState(10.5)
        )
    ) AS nested_metrics;

-- Insert second batch with same key (should trigger aggregation)
INSERT INTO test_nested_tuple_agg
SELECT
    1 AS id,
    tuple(
        tuple(
            200,
            tuple(
                75,
                tuple(
                    5,
                    uniqState('value2'),
                    tuple(
                        'second',
                        countState()
                    )
                )
            )
        ),
        tuple(
            2,
            avgState(20.5)
        )
    ) AS nested_metrics;

-- Insert third batch
INSERT INTO test_nested_tuple_agg
SELECT
    1 AS id,
    tuple(
        tuple(
            50,
            tuple(
                100,
                tuple(
                    15,
                    uniqState('value3'),
                    tuple(
                        'third',
                        countState()
                    )
                )
            )
        ),
        tuple(
            4,
            avgState(15.0)
        )
    ) AS nested_metrics;

-- Insert data for different key
INSERT INTO test_nested_tuple_agg
SELECT
    2 AS id,
    tuple(
        tuple(
            1000,
            tuple(
                500,
                tuple(
                    100,
                    uniqState('alpha'),
                    tuple(
                        'alpha_last',
                        countState()
                    )
                )
            )
        ),
        tuple(
            8,
            avgState(5.5)
        )
    ) AS nested_metrics;

-- Test nested tuple aggregation with FINAL
SELECT 'Nested Tuple test - with FINAL:';
SELECT
    id,
    nested_metrics.level1.simple_sum AS l1_sum,
    nested_metrics.level1.level2.simple_max AS l2_max,
    nested_metrics.level1.level2.level3.simple_min AS l3_min,
    uniqMerge(nested_metrics.level1.level2.level3.agg_uniq) AS l3_unique_count,
    nested_metrics.level1.level2.level3.level4.simple_any_last AS l4_any_last,
    countMerge(nested_metrics.level1.level2.level3.level4.agg_count) AS l4_count,
    nested_metrics.parallel_branch.simple_bit_or AS parallel_bit_or,
    avgMerge(nested_metrics.parallel_branch.agg_avg) AS parallel_avg
FROM test_nested_tuple_agg FINAL
GROUP BY
    id,
    nested_metrics.level1.simple_sum,
    nested_metrics.level1.level2.simple_max,
    nested_metrics.level1.level2.level3.simple_min,
    nested_metrics.level1.level2.level3.level4.simple_any_last,
    nested_metrics.parallel_branch.simple_bit_or
ORDER BY id;

-- Test after OPTIMIZE
OPTIMIZE TABLE test_nested_tuple_agg FINAL;

SELECT 'Nested Tuple test - after OPTIMIZE:';
SELECT
    id,
    nested_metrics.level1.simple_sum AS l1_sum,
    nested_metrics.level1.level2.simple_max AS l2_max,
    nested_metrics.level1.level2.level3.simple_min AS l3_min,
    uniqMerge(nested_metrics.level1.level2.level3.agg_uniq) AS l3_unique_count,
    nested_metrics.level1.level2.level3.level4.simple_any_last AS l4_any_last,
    countMerge(nested_metrics.level1.level2.level3.level4.agg_count) AS l4_count,
    nested_metrics.parallel_branch.simple_bit_or AS parallel_bit_or,
    avgMerge(nested_metrics.parallel_branch.agg_avg) AS parallel_avg
FROM test_nested_tuple_agg
GROUP BY
    id,
    nested_metrics.level1.simple_sum,
    nested_metrics.level1.level2.simple_max,
    nested_metrics.level1.level2.level3.simple_min,
    nested_metrics.level1.level2.level3.level4.simple_any_last,
    nested_metrics.parallel_branch.simple_bit_or
ORDER BY id;

DROP TABLE test_nested_tuple_agg;

-- Test 2: ColumnConst with Tuple aggregation
SELECT '=== Test 2: ColumnConst with Tuple aggregation ===';
DROP TABLE IF EXISTS test_const_tuple_agg;

CREATE TABLE test_const_tuple_agg (
    id UInt32,
    const_metrics Tuple(
        simple_sum SimpleAggregateFunction(sum, UInt64),
        simple_max SimpleAggregateFunction(max, UInt64),
        agg_uniq AggregateFunction(uniq, String),
        nested Tuple(
            simple_any_last SimpleAggregateFunction(anyLast, String),
            agg_count AggregateFunction(count)
        )
    )
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert data with constant tuple values using materialize
INSERT INTO test_const_tuple_agg
SELECT
    number % 3 AS id,
    materialize(
        tuple(
            100,
            50,
            uniqState('constant_value'),
            tuple(
                'const_string',
                countState()
            )
        )
    ) AS const_metrics
FROM numbers(10)
GROUP BY id;

-- Insert more data with different constant values
INSERT INTO test_const_tuple_agg
SELECT
    number % 3 AS id,
    materialize(
        tuple(
            200,
            75,
            uniqState('another_constant'),
            tuple(
                'another_const',
                countState()
            )
        )
    ) AS const_metrics
FROM numbers(10)
GROUP BY id;

-- Test with constant tuple columns before OPTIMIZE
SELECT 'ColumnConst Tuple test - WITH FINAL:';
SELECT
    id,
    const_metrics.simple_sum AS sum_val,
    const_metrics.simple_max AS max_val,
    uniqMerge(const_metrics.agg_uniq) AS unique_count,
    const_metrics.nested.simple_any_last AS nested_any_last,
    countMerge(const_metrics.nested.agg_count) AS nested_count
FROM test_const_tuple_agg final
GROUP BY id, const_metrics.simple_sum, const_metrics.simple_max, const_metrics.nested.simple_any_last
ORDER BY id;

-- Test after OPTIMIZE
OPTIMIZE TABLE test_const_tuple_agg FINAL;

SELECT 'ColumnConst Tuple test - after OPTIMIZE:';
SELECT
    id,
    const_metrics.simple_sum AS sum_val,
    const_metrics.simple_max AS max_val,
    uniqMerge(const_metrics.agg_uniq) AS unique_count,
    const_metrics.nested.simple_any_last AS nested_any_last,
    countMerge(const_metrics.nested.agg_count) AS nested_count
FROM test_const_tuple_agg
GROUP BY id, const_metrics.simple_sum, const_metrics.simple_max, const_metrics.nested.simple_any_last
ORDER BY id;

DROP TABLE test_const_tuple_agg;

-- Test 3: Nested Tuple column before sorting key column (removeReplicatedFromSortingColumns bug)
-- When tuple columns precede the sort key in schema, flattening shifts column indices.
SELECT '=== Test 3: Nested Tuple before sorting key ===';
DROP TABLE IF EXISTS test_nested_tuple_before_sort_key;

CREATE TABLE test_nested_tuple_before_sort_key (
    nested_data Tuple(
        level1 Tuple(
            simple_sum SimpleAggregateFunction(sum, UInt64),
            level2 Tuple(
                simple_max SimpleAggregateFunction(max, UInt64),
                agg_count AggregateFunction(count)
            )
        )
    ),
    id UInt32
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert first batch
INSERT INTO test_nested_tuple_before_sort_key
SELECT
    tuple(tuple(100, tuple(50, countState()))) AS nested_data,
    1 AS id;

INSERT INTO test_nested_tuple_before_sort_key
SELECT
    tuple(tuple(200, tuple(80, countState()))) AS nested_data,
    2 AS id;

-- Insert second batch with same keys to trigger merge aggregation
INSERT INTO test_nested_tuple_before_sort_key
SELECT
    tuple(tuple(150, tuple(60, countState()))) AS nested_data,
    1 AS id;

INSERT INTO test_nested_tuple_before_sort_key
SELECT
    tuple(tuple(250, tuple(90, countState()))) AS nested_data,
    2 AS id;

SELECT 'Nested Tuple before sort key - with FINAL:';
SELECT
    id,
    nested_data.level1.simple_sum AS l1_sum,
    nested_data.level1.level2.simple_max AS l2_max,
    countMerge(nested_data.level1.level2.agg_count) AS l2_count
FROM test_nested_tuple_before_sort_key FINAL
GROUP BY id, nested_data.level1.simple_sum, nested_data.level1.level2.simple_max
ORDER BY id;

OPTIMIZE TABLE test_nested_tuple_before_sort_key FINAL;

SELECT 'Nested Tuple before sort key - after OPTIMIZE:';
SELECT
    id,
    nested_data.level1.simple_sum AS l1_sum,
    nested_data.level1.level2.simple_max AS l2_max,
    countMerge(nested_data.level1.level2.agg_count) AS l2_count
FROM test_nested_tuple_before_sort_key
GROUP BY id, nested_data.level1.simple_sum, nested_data.level1.level2.simple_max
ORDER BY id;

DROP TABLE test_nested_tuple_before_sort_key;
