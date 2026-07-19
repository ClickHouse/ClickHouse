-- The existing window-function form remains available without the experimental setting.
SELECT exponentialTimeDecayedSum(10)(value, time) OVER ()
FROM VALUES('value Float64, time Float64', (1, 0));

-- The new aggregate-function form is experimental and disabled by default.
SELECT exponentialTimeDecayedSum(10)(toFloat64(1), toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET allow_experimental_time_decay_aggregate_functions = 1;

SELECT
    round(exponentialTimeDecayedSum(10)(value, time), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(exponentialTimeDecayedCount(10)(time), 6)
FROM VALUES('value Float64, time Float64', (10, 0), (20, 10), (5, 5));

-- The aggregation result must not depend on row order.
SELECT
    round(exponentialTimeDecayedSum(10)(value, time), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(exponentialTimeDecayedCount(10)(time), 6)
FROM
(
    SELECT *
    FROM VALUES('value Float64, time Float64', (10, 0), (20, 10), (5, 5))
    ORDER BY time ASC
)
UNION ALL
SELECT
    round(exponentialTimeDecayedSum(10)(value, time), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(exponentialTimeDecayedCount(10)(time), 6)
FROM
(
    SELECT *
    FROM VALUES('value Float64, time Float64', (10, 0), (20, 10), (5, 5))
    ORDER BY time DESC
);

-- Independently aggregated states must merge to the same result.
SELECT
    round(exponentialTimeDecayedSumMerge(10)(sum_state), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(exponentialTimeDecayedCountMerge(10)(count_state), 6)
FROM
(
    SELECT
        exponentialTimeDecayedSumState(10)(value, time) AS sum_state,
        exponentialTimeDecayedAvgState(10)(value, time) AS avg_state,
        exponentialTimeDecayedCountState(10)(time) AS count_state
    FROM VALUES('value Float64, time Float64', (10, 0), (5, 5))
    UNION ALL
    SELECT
        exponentialTimeDecayedSumState(10)(value, time) AS sum_state,
        exponentialTimeDecayedAvgState(10)(value, time) AS avg_state,
        exponentialTimeDecayedCountState(10)(time) AS count_state
    FROM VALUES('value Float64, time Float64', (20, 10))
);

-- A different two-batch distribution must produce the same result.
SELECT
    round(exponentialTimeDecayedSumMerge(10)(sum_state), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(exponentialTimeDecayedCountMerge(10)(count_state), 6)
FROM
(
    SELECT
        exponentialTimeDecayedSumState(10)(value, time) AS sum_state,
        exponentialTimeDecayedAvgState(10)(value, time) AS avg_state,
        exponentialTimeDecayedCountState(10)(time) AS count_state
    FROM VALUES('value Float64, time Float64', (10, 0), (20, 10))
    UNION ALL
    SELECT
        exponentialTimeDecayedSumState(10)(value, time) AS sum_state,
        exponentialTimeDecayedAvgState(10)(value, time) AS avg_state,
        exponentialTimeDecayedCountState(10)(time) AS count_state
    FROM VALUES('value Float64, time Float64', (5, 5))
);

-- Merging one state per input row must also produce the same result.
SELECT
    round(exponentialTimeDecayedSumMerge(10)(sum_state), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(exponentialTimeDecayedCountMerge(10)(count_state), 6)
FROM
(
    SELECT
        batch,
        exponentialTimeDecayedSumState(10)(value, time) AS sum_state,
        exponentialTimeDecayedAvgState(10)(value, time) AS avg_state,
        exponentialTimeDecayedCountState(10)(time) AS count_state
    FROM VALUES(
        'batch UInt8, value Float64, time Float64',
        (0, 10, 0),
        (1, 20, 10),
        (2, 5, 5))
    GROUP BY batch
);

DROP TABLE IF EXISTS exponential_time_decayed_aggregate;

CREATE TABLE exponential_time_decayed_aggregate
(
    key UInt8,
    sum_state AggregateFunction(exponentialTimeDecayedSum(10), Float64, Float64),
    avg_state AggregateFunction(exponentialTimeDecayedAvg(10), Float64, Float64),
    count_state AggregateFunction(exponentialTimeDecayedCount(10), Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO exponential_time_decayed_aggregate
SELECT
    1,
    exponentialTimeDecayedSumState(10)(value, time),
    exponentialTimeDecayedAvgState(10)(value, time),
    exponentialTimeDecayedCountState(10)(time)
FROM VALUES('value Float64, time Float64', (20, 10));

INSERT INTO exponential_time_decayed_aggregate
SELECT
    1,
    exponentialTimeDecayedSumState(10)(value, time),
    exponentialTimeDecayedAvgState(10)(value, time),
    exponentialTimeDecayedCountState(10)(time)
FROM VALUES('value Float64, time Float64', (10, 0), (5, 5));

OPTIMIZE TABLE exponential_time_decayed_aggregate FINAL;

SELECT
    round(exponentialTimeDecayedSumMerge(10)(sum_state), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(exponentialTimeDecayedCountMerge(10)(count_state), 6)
FROM exponential_time_decayed_aggregate;

DROP TABLE exponential_time_decayed_aggregate;

-- Decimal values and DateTime64 time arguments use their scaled values.
SELECT
    round(exponentialTimeDecayedSum(10)(value, time), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(exponentialTimeDecayedCount(10)(time), 6)
FROM VALUES(
    'value Decimal64(2), time DateTime64(3)',
    (10, '2020-01-01 00:00:00'),
    (20, '2020-01-01 00:00:10'),
    (5, '2020-01-01 00:00:05'));

SELECT exponentialTimeDecayedSum(0)(1, 1); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedAvg(-1)(1, 1); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedCount(10)('not a time'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT exponentialTimeDecayedSum(10)('not a value', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Property-style test: direct aggregation and merging an arbitrary number of
-- independently aggregated batches must be numerically equivalent.
WITH
    source AS
    (
        SELECT
            number,
            toFloat64(((number * 13) % 101) + 1) AS value,
            toFloat64((number * 104729) % 100000) / 10 AS time
        FROM numbers(4096)
    ),
    direct AS
    (
        SELECT
            exponentialTimeDecayedSum(250)(value, time) AS expected_sum,
            exponentialTimeDecayedAvg(250)(value, time) AS expected_avg,
            exponentialTimeDecayedCount(250)(time) AS expected_count
        FROM source
    ),
    batch_states AS
    (
        SELECT
            batch_count,
            number % batch_count AS batch_id,
            exponentialTimeDecayedSumState(250)(value, time) AS sum_state,
            exponentialTimeDecayedAvgState(250)(value, time) AS avg_state,
            exponentialTimeDecayedCountState(250)(time) AS count_state
        FROM source
        CROSS JOIN
        (
            SELECT arrayJoin([1, 2, 17, 257, 4096]) AS batch_count
        )
        GROUP BY
            batch_count,
            batch_id
    ),
    merged AS
    (
        SELECT
            batch_count,
            exponentialTimeDecayedSumMerge(250)(sum_state) AS actual_sum,
            exponentialTimeDecayedAvgMerge(250)(avg_state) AS actual_avg,
            exponentialTimeDecayedCountMerge(250)(count_state) AS actual_count
        FROM batch_states
        GROUP BY batch_count
    )
SELECT
    batch_count,
    abs(actual_sum - expected_sum) <= 1e-12 * greatest(1., abs(expected_sum)),
    abs(actual_avg - expected_avg) <= 1e-12 * greatest(1., abs(expected_avg)),
    abs(actual_count - expected_count) <= 1e-12 * greatest(1., abs(expected_count))
FROM merged
CROSS JOIN direct
ORDER BY batch_count;
