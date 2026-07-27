SET optimize_throw_if_noop = 1;

-- Test 1: AggregateFunction subcolumns (state-based aggregation)
SELECT '=== Test 1: AggregateFunction subcolumns ===';
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

-- Test 2: With overflow
SELECT '=== Test 2: With overflow ===';
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
