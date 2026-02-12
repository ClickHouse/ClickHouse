SET optimize_throw_if_noop = 1;

-- Test 1: Basic SimpleAggregateFunction subcolumn test
SELECT '=== Test 1: Basic SimpleAggregateFunction subcolumn test ===';
DROP TABLE IF EXISTS test_simple_subcolumn;

CREATE TABLE test_simple_subcolumn (
    metrics Tuple(
        id UInt64,
        val SimpleAggregateFunction(sum, Double)
    )
) ENGINE = AggregatingMergeTree() ORDER BY metrics.id
SETTINGS allow_tuple_element_aggregation = 1;

-- First insert
INSERT INTO test_simple_subcolumn SELECT tuple(number, number) FROM numbers(10);

-- Second insert to test merge behavior
INSERT INTO test_simple_subcolumn SELECT tuple(number, number) FROM numbers(10);

-- Test with FINAL to verify immediate aggregation
SELECT 'Basic test - with FINAL (after two inserts):';
SELECT * FROM test_simple_subcolumn FINAL ORDER BY metrics.id;

-- Test OPTIMIZE to verify on-disk aggregation
OPTIMIZE TABLE test_simple_subcolumn FINAL;

SELECT 'Basic test - after OPTIMIZE:';
SELECT * FROM test_simple_subcolumn ORDER BY metrics.id;

DROP TABLE test_simple_subcolumn;


-- Test 2: Comprehensive SimpleAggregateFunction types in subcolumns
SELECT '=== Test 2: Comprehensive SimpleAggregateFunction types ===';
DROP TABLE IF EXISTS test_comprehensive_simple;

CREATE TABLE test_comprehensive_simple (
    id UInt64,
    agg_tuple Tuple(
        sum_val SimpleAggregateFunction(sum, UInt64),
        min_val SimpleAggregateFunction(min, UInt64),
        max_val SimpleAggregateFunction(max, UInt64),
        any_val SimpleAggregateFunction(any, String),
        any_last_val SimpleAggregateFunction(anyLast, String),
        nullable_str SimpleAggregateFunction(anyLast, Nullable(String)),
        nullable_str_respect_nulls SimpleAggregateFunction(anyLastRespectNulls, Nullable(String)),
        low_str SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))),
        ip_addr SimpleAggregateFunction(anyLast, IPv4),
        status SimpleAggregateFunction(groupBitOr, UInt32),
        bit_and SimpleAggregateFunction(groupBitAnd, UInt32),
        bit_xor SimpleAggregateFunction(groupBitXor, UInt32),
        arr SimpleAggregateFunction(groupArrayArray, Array(Int32)),
        uniq_arr SimpleAggregateFunction(groupUniqArrayArray, Array(Int32))
    )
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert first batch
INSERT INTO test_comprehensive_simple VALUES(
    1,
    tuple(
        100, 10, 50, 'first', 'first_last', '1', '1', '1', '192.168.1.1', 
        1, 15, 5, [1,2], [1,2]
    )
);

-- Insert second batch with same key (should trigger aggregation)
INSERT INTO test_comprehensive_simple VALUES(
    1, 
    tuple(
        200, 5, 100, 'second', 'second_last', null, null, '2', '192.168.1.2', 
        2, 7, 3, [2,3,4], [2,3,4]
    )
);

-- Insert third batch
INSERT INTO test_comprehensive_simple VALUES(
    1, 
    tuple(
        50, 15, 75, 'third', 'third_last', '3', '3', '3', '192.168.1.3', 
        4, 3, 7, [5], [5,6]
    )
);

-- Insert data for id=10 with long string (longer than MAX_SMALL_STRING_SIZE)
INSERT INTO test_comprehensive_simple VALUES(
    10, 
    tuple(
        1000, 100, 500, 'ten', 'ten_last', '10', null, '10', '10.0.0.1', 
        8, 255, 15, [], []
    )
);

INSERT INTO test_comprehensive_simple VALUES(
    10, 
    tuple(
        2000, 50, 1000, 'twenty', '2222',
        '2222',
        '10', '20', '10.0.0.2', 
        16, 127, 31, [], []
    )
);

-- Test immediate aggregation with FINAL
SELECT 'Comprehensive test - with FINAL:';
SELECT
    id,
    agg_tuple.sum_val,
    agg_tuple.min_val,
    agg_tuple.max_val,
    agg_tuple.any_val,
    agg_tuple.any_last_val,
    agg_tuple.nullable_str,
    agg_tuple.nullable_str_respect_nulls,
    agg_tuple.low_str,
    agg_tuple.ip_addr,
    agg_tuple.status,
    agg_tuple.bit_and,
    agg_tuple.bit_xor,
    agg_tuple.arr,
    arraySort(agg_tuple.uniq_arr) AS uniq_arr
FROM test_comprehensive_simple FINAL ORDER BY id;

-- Test on-disk aggregation with OPTIMIZE
OPTIMIZE TABLE test_comprehensive_simple FINAL;

SELECT 'Comprehensive test - after OPTIMIZE:';
SELECT
    id,
    agg_tuple.sum_val,
    agg_tuple.min_val,
    agg_tuple.max_val,
    agg_tuple.any_val,
    agg_tuple.any_last_val,
    agg_tuple.nullable_str,
    agg_tuple.nullable_str_respect_nulls,
    agg_tuple.low_str,
    agg_tuple.ip_addr,
    agg_tuple.status,
    agg_tuple.bit_and,
    agg_tuple.bit_xor,
    agg_tuple.arr,
    arraySort(agg_tuple.uniq_arr) AS uniq_arr
FROM test_comprehensive_simple ORDER BY id;

DROP TABLE test_comprehensive_simple;

-- Test 3: AggregateFunction subcolumns (state-based aggregation)
SELECT '=== Test 3: AggregateFunction subcolumns ===';
DROP TABLE IF EXISTS test_agg_function_subcolumn;

CREATE TABLE test_agg_function_subcolumn (
    id UInt64,
    agg_states Tuple(
        uniq_state AggregateFunction(uniq, UInt64),
        uniq_exact_state AggregateFunction(uniqExact, String),
        sum_state AggregateFunction(sum, UInt64),
        avg_state AggregateFunction(avg, Float64),
        count_state AggregateFunction(count),
        min_state AggregateFunction(min, Int64),
        max_state AggregateFunction(max, Int64),
        arg_min_state AggregateFunction(argMin, String, UInt64),
        arg_max_state AggregateFunction(argMax, String, UInt64),
        quantile_state AggregateFunction(quantile(0.5), Float64),
        group_array_state AggregateFunction(groupArray, String)
    )
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert first batch with aggregate states
INSERT INTO test_agg_function_subcolumn
SELECT
    number % 3 AS id,
    tuple(
        uniqState(toUInt64(number)),
        uniqExactState(toString(number)),
        sumState(toUInt64(number * 10)),
        avgState(toFloat64(number)),
        countState(),
        minState(toInt64(number)),
        maxState(toInt64(number)),
        argMinState(concat('val_', toString(number)), toUInt64(number)),
        argMaxState(concat('val_', toString(number)), toUInt64(number)),
        quantileState(0.5)(toFloat64(number)),
        groupArrayState(toString(number))
    ) AS agg_states
FROM numbers(10)
GROUP BY id;

-- Insert second batch with same keys (should trigger state merging)
INSERT INTO test_agg_function_subcolumn
SELECT
    number % 3 AS id,
    tuple(
        uniqState(toUInt64(number + 100)),
        uniqExactState(concat('str_', toString(number))),
        sumState(toUInt64(number * 10)),
        avgState(toFloat64(number + 50)),
        countState(),
        minState(toInt64(number - 5)),
        maxState(toInt64(number + 5)),
        argMinState(concat('new_', toString(number)), toUInt64(number + 10)),
        argMaxState(concat('new_', toString(number)), toUInt64(number + 10)),
        quantileState(0.5)(toFloat64(number + 10)),
        groupArrayState(concat('new_', toString(number)))
    ) AS agg_states
FROM numbers(10)
GROUP BY id;

-- Test state merging before OPTIMIZE
SELECT 'AggregateFunction test - before OPTIMIZE:';
SELECT
    id,
    uniqMerge(agg_states.uniq_state) AS unique_count,
    uniqExactMerge(agg_states.uniq_exact_state) AS unique_exact_count,
    sumMerge(agg_states.sum_state) AS total_sum,
    avgMerge(agg_states.avg_state) AS avg_val,
    countMerge(agg_states.count_state) AS total_count,
    minMerge(agg_states.min_state) AS min_val,
    maxMerge(agg_states.max_state) AS max_val,
    argMinMerge(agg_states.arg_min_state) AS arg_min_val,
    argMaxMerge(agg_states.arg_max_state) AS arg_max_val,
    quantileMerge(0.5)(agg_states.quantile_state) AS median_val,
    length(groupArrayMerge(agg_states.group_array_state)) AS array_length
FROM test_agg_function_subcolumn
GROUP BY id
ORDER BY id;

-- Test state merging after OPTIMIZE
OPTIMIZE TABLE test_agg_function_subcolumn FINAL;

SELECT 'AggregateFunction test - after OPTIMIZE:';
SELECT
    id,
    uniqMerge(agg_states.uniq_state) AS unique_count,
    uniqExactMerge(agg_states.uniq_exact_state) AS unique_exact_count,
    sumMerge(agg_states.sum_state) AS total_sum,
    avgMerge(agg_states.avg_state) AS avg_val,
    countMerge(agg_states.count_state) AS total_count,
    minMerge(agg_states.min_state) AS min_val,
    maxMerge(agg_states.max_state) AS max_val,
    argMinMerge(agg_states.arg_min_state) AS arg_min_val,
    argMaxMerge(agg_states.arg_max_state) AS arg_max_val,
    quantileMerge(0.5)(agg_states.quantile_state) AS median_val,
    length(groupArrayMerge(agg_states.group_array_state)) AS array_length
FROM test_agg_function_subcolumn
GROUP BY id
ORDER BY id;

DROP TABLE test_agg_function_subcolumn;

-- Test 4: With overflow
SELECT '=== Test 4: With overflow ===';
DROP TABLE IF EXISTS test_with_overflow;

CREATE TABLE test_with_overflow (
    id UInt64,
    overflow_agg Tuple(
        sum_overflow SimpleAggregateFunction(sumWithOverflow, UInt8)
    )
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_with_overflow SELECT 1, tuple(1) FROM numbers(256);

OPTIMIZE TABLE test_with_overflow FINAL;

SELECT 'with_overflow', * FROM test_with_overflow ORDER BY id;

DROP TABLE test_with_overflow;

-- Test 5: Multi-level nested Tuple aggregation
SELECT '=== Test 5: Multi-level nested Tuple aggregation ===';
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

-- Test 6: ColumnConst with Tuple aggregation
SELECT '=== Test 6: ColumnConst with Tuple aggregation ===';
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

-- Test 7: Unnamed Tuple (positional access) with aggregation
SELECT '=== Test 7: Unnamed Tuple (positional access) ===';
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

-- Test with nested unnamed tuple
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
DROP TABLE test_unnamed_tuple_agg;

