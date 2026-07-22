-- The existing window-function form remains available without the experimental setting.
SELECT exponentialTimeDecayedSum(10)(value, time) OVER ()
FROM VALUES('value Float64, time Float64', (1, 0));

-- The new aggregate-function form is experimental and disabled by default.
SELECT exponentialTimeDecayedSum(10)(toFloat64(1), toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT exponentialTimeDecayingFloat64(1, toFloat64(0), 10); -- { serverError UNKNOWN_FUNCTION }

-- Boolean keywords enable and disable the aggregate-function forms.
SET allow_experimental_time_decay_aggregate_functions = true;
SELECT tupleElement(exponentialTimeDecayedSum(10)(toFloat64(1), toFloat64(0)), 'value');

SET allow_experimental_time_decay_aggregate_functions = false;
SELECT exponentialTimeDecayedAvg(10)(toFloat64(1), toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

-- Numeric Boolean values have the same behavior.
SET allow_experimental_time_decay_aggregate_functions = 0;
SELECT exponentialTimeDecayedCount(10)(toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT exponentialTimeDecayingFloat64(1, toFloat64(0), 10); -- { serverError UNKNOWN_FUNCTION }

SET allow_experimental_time_decay_aggregate_functions = 1;

-- A value observed exactly one half-life before the greatest timestamp has
-- weight 1/2. Aggregate and window execution must use the same definition.
SELECT
    round(tupleElement(exponentialTimeDecayedSum(10)(value, time), 'value'), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(tupleElement(exponentialTimeDecayedCount(10)(time), 'value'), 6)
FROM VALUES('value Float64, time Float64', (2, 0), (0, 10));

SELECT weighted_sum, weighted_avg, weight
FROM
(
    SELECT
        time,
        round(exponentialTimeDecayedSum(10)(value, time) OVER w, 6) AS weighted_sum,
        round(exponentialTimeDecayedAvg(10)(value, time) OVER w, 6) AS weighted_avg,
        round(exponentialTimeDecayedCount(10)(time) OVER w, 6) AS weight
    FROM VALUES('value Float64, time Float64', (2, 0), (0, 10))
    WINDOW w AS (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
ORDER BY time DESC
LIMIT 1;

SELECT
    round(tupleElement(exponentialTimeDecayedSum(10)(value, time), 'value'), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(tupleElement(exponentialTimeDecayedCount(10)(time), 'value'), 6)
FROM VALUES('value Float64, time Float64', (10, 0), (20, 10), (5, 5));

-- The aggregation result must not depend on row order.
SELECT
    round(tupleElement(exponentialTimeDecayedSum(10)(value, time), 'value'), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(tupleElement(exponentialTimeDecayedCount(10)(time), 'value'), 6)
FROM
(
    SELECT *
    FROM VALUES('value Float64, time Float64', (10, 0), (20, 10), (5, 5))
    ORDER BY time ASC
)
UNION ALL
SELECT
    round(tupleElement(exponentialTimeDecayedSum(10)(value, time), 'value'), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(tupleElement(exponentialTimeDecayedCount(10)(time), 'value'), 6)
FROM
(
    SELECT *
    FROM VALUES('value Float64, time Float64', (10, 0), (20, 10), (5, 5))
    ORDER BY time DESC
);

-- Independently aggregated states must merge to the same result.
SELECT
    round(tupleElement(exponentialTimeDecayedSumMerge(10)(sum_state), 'value'), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(tupleElement(exponentialTimeDecayedCountMerge(10)(count_state), 'value'), 6)
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
    round(tupleElement(exponentialTimeDecayedSumMerge(10)(sum_state), 'value'), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(tupleElement(exponentialTimeDecayedCountMerge(10)(count_state), 'value'), 6)
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
    round(tupleElement(exponentialTimeDecayedSumMerge(10)(sum_state), 'value'), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(tupleElement(exponentialTimeDecayedCountMerge(10)(count_state), 'value'), 6)
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
    round(tupleElement(exponentialTimeDecayedSumMerge(10)(sum_state), 'value'), 6),
    round(exponentialTimeDecayedAvgMerge(10)(avg_state), 6),
    round(tupleElement(exponentialTimeDecayedCountMerge(10)(count_state), 'value'), 6)
FROM exponential_time_decayed_aggregate;

DROP TABLE exponential_time_decayed_aggregate;

-- Decimal values and DateTime64 time arguments use their scaled values.
SELECT
    round(tupleElement(decaying_sum, 'value'), 6),
    round(decaying_avg, 6),
    round(tupleElement(decaying_count, 'value'), 6),
    toTypeName(decaying_sum),
    tupleElement(decaying_sum, 'time'),
    tupleElement(decaying_sum, 'half_life')
FROM
(
    SELECT
        exponentialTimeDecayedSum(10)(value, time) AS decaying_sum,
        exponentialTimeDecayedAvg(10)(value, time) AS decaying_avg,
        exponentialTimeDecayedCount(10)(time) AS decaying_count
    FROM VALUES(
        'value Decimal64(2), time DateTime64(3)',
        (10, '2020-01-01 00:00:00'),
        (20, '2020-01-01 00:00:10'),
        (5, '2020-01-01 00:00:05'))
);

-- Decaying values with the same half-life can be combined at their latest anchor.
WITH
    exponentialTimeDecayingFloat64(8, toFloat64(0), 10) AS a,
    exponentialTimeDecayingFloat64(4, toFloat64(10), 10) AS b,
    exponentialTimeDecayingAdd(a, b) AS c
SELECT
    toTypeName(c),
    round(tupleElement(c, 'value'), 6),
    tupleElement(c, 'time'),
    round(tupleElement(c, 'half_life'), 6),
    round(exponentialTimeDecayingValueAt(c, toFloat64(20)), 6);

-- DateTime anchors retain their type and use seconds as the half-life unit.
WITH exponentialTimeDecayingFloat64(
    8,
    toDateTime('2020-01-01 00:00:00', 'UTC'),
    10) AS decaying_value
SELECT
    tupleElement(decaying_value, 'time'),
    round(exponentialTimeDecayingValueAt(
        decaying_value,
        toDateTime('2020-01-01 00:00:10', 'UTC')), 6);

-- Rebasing recalculates the value while preserving the half-life and original DateTime64 type.
WITH
    exponentialTimeDecayingFloat64(8, toDateTime64('2020-01-01 00:00:00', 3), 10) AS a,
    exponentialTimeDecayingFloat64(4, toDateTime64('2020-01-01 00:00:10', 3), 10) AS b,
    exponentialTimeDecayingRebase(
        a + b,
        toDateTime64('2020-01-01 00:00:20', 3)) AS rebased
SELECT
    toTypeName(rebased),
    tupleElement(rebased, 'time'),
    round(tupleElement(rebased, 'value'), 6),
    round(tupleElement(rebased, 'half_life'), 6);

-- The parameterized type can be stored while preserving its exact time type.
DROP TABLE IF EXISTS exponential_time_decaying_values;
CREATE TABLE exponential_time_decaying_values
(
    decaying_value ExponentialTimeDecayingFloat64(DateTime64(3))
)
ENGINE = Memory;

INSERT INTO exponential_time_decaying_values
WITH
    exponentialTimeDecayingFloat64(8, toDateTime64('2020-01-01 00:00:00', 3), 10) AS a,
    exponentialTimeDecayingFloat64(4, toDateTime64('2020-01-01 00:00:10', 3), 10) AS b
SELECT a + b;

SELECT
    round(tupleElement(decaying_value, 'value'), 6),
    tupleElement(decaying_value, 'time'),
    round(tupleElement(decaying_value, 'half_life'), 6),
    round(exponentialTimeDecayingValueAt(
        decaying_value,
        toDateTime64('2020-01-01 00:00:20', 3)), 6)
FROM exponential_time_decaying_values;

DROP TABLE exponential_time_decaying_values;

-- Different half-lives cannot be represented by one decay curve and are rejected.
SELECT exponentialTimeDecayingAdd(
    exponentialTimeDecayingFloat64(8, toFloat64(0), 10),
    exponentialTimeDecayingFloat64(4, toFloat64(10), 20)); -- { serverError BAD_ARGUMENTS }
SELECT
    exponentialTimeDecayingFloat64(8, toFloat64(0), 10)
    + exponentialTimeDecayingFloat64(4, toFloat64(10), 20); -- { serverError BAD_ARGUMENTS }

-- Repeated addition with the same half-life remains independent of grouping.
WITH
    exponentialTimeDecayingFloat64(1, toFloat64(0), 10) AS a,
    exponentialTimeDecayingFloat64(1, toFloat64(1), 10) AS b,
    exponentialTimeDecayingFloat64(1, toFloat64(2), 10) AS c,
    (a + b) + c AS lhs,
    a + (b + c) AS rhs
SELECT
    abs(tupleElement(lhs, 'value') - tupleElement(rhs, 'value')) < 1e-12,
    abs(tupleElement(lhs, 'half_life') - tupleElement(rhs, 'half_life')) < 1e-12,
    abs(exponentialTimeDecayingValueAt(lhs, toFloat64(10)) - exponentialTimeDecayingValueAt(rhs, toFloat64(10))) < 1e-12;

-- Reproducibly generated values with different timestamps and the same half-life
-- must be independent of input order and intermediate batch grouping.
WITH
    arrayMap(
        number -> exponentialTimeDecayingFloat64(
            toFloat64((sipHash64(number, 11) % 100000) + 1) / 1000,
            toFloat64(sipHash64(number, 22) % 100000) / 100,
            toFloat64(17)),
        range(64)) AS values,
    arrayFold(
        (acc, value) -> acc + value,
        arraySlice(values, 2),
        values[1]) AS direct,
    arrayReverse(values) AS reversed_values,
    arrayFold(
        (acc, value) -> acc + value,
        arraySlice(reversed_values, 2),
        reversed_values[1]) AS reversed,
    arraySlice(values, 1, 32) AS first_batch_values,
    arraySlice(values, 33, 32) AS second_batch_values,
    arrayFold(
        (acc, value) -> acc + value,
        arraySlice(first_batch_values, 2),
        first_batch_values[1]) AS first_batch,
    arrayFold(
        (acc, value) -> acc + value,
        arraySlice(second_batch_values, 2),
        second_batch_values[1]) AS second_batch,
    first_batch + second_batch AS batched
SELECT
    abs(tupleElement(direct, 'value') - tupleElement(reversed, 'value')) <= 1e-12 * greatest(1., tupleElement(direct, 'value')),
    abs(tupleElement(direct, 'half_life') - tupleElement(batched, 'half_life')) <= 1e-12 * greatest(1., tupleElement(direct, 'half_life')),
    abs(exponentialTimeDecayingValueAt(direct, toFloat64(2000)) - exponentialTimeDecayingValueAt(batched, toFloat64(2000)))
        <= 1e-12 * greatest(1., exponentialTimeDecayingValueAt(direct, toFloat64(2000)));

SELECT exponentialTimeDecayedSum(0)(1, 1); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedAvg(-1)(1, 1); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedCount(10)('not a time'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT exponentialTimeDecayedSum(10)('not a value', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT exponentialTimeDecayedSum(10)(-1, 1); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayingFloat64(-1, toFloat64(0), 10); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayingFloat64(1, toFloat64(0), 0); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayingValueAt(
    exponentialTimeDecayingFloat64(1, toFloat64(10), 10),
    toFloat64(0)); -- { serverError BAD_ARGUMENTS }

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
            tupleElement(exponentialTimeDecayedSum(250)(value, time), 'value') AS expected_sum,
            exponentialTimeDecayedAvg(250)(value, time) AS expected_avg,
            tupleElement(exponentialTimeDecayedCount(250)(time), 'value') AS expected_count
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
            tupleElement(exponentialTimeDecayedSumMerge(250)(sum_state), 'value') AS actual_sum,
            exponentialTimeDecayedAvgMerge(250)(avg_state) AS actual_avg,
            tupleElement(exponentialTimeDecayedCountMerge(250)(count_state), 'value') AS actual_count
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

-- Generate reproducible pseudo-random values, timestamps, row orders, and batch
-- assignments. Both randomized input orders and all batch distributions must
-- produce numerically equivalent results.
WITH
    source AS
    (
        SELECT
            number,
            toFloat64((sipHash64(number, 101) % 1000000) + 1) / 1000 AS value,
            toFloat64(sipHash64(number, 202) % 10000000) / 1000 AS time,
            sipHash64(number, 303) AS first_order,
            sipHash64(number, 404) AS second_order,
            sipHash64(number, 505) AS batch_key
        FROM numbers(2048)
    ),
    first_direct AS
    (
        SELECT
            tupleElement(exponentialTimeDecayedSum(500)(value, time), 'value') AS expected_sum,
            exponentialTimeDecayedAvg(500)(value, time) AS expected_avg,
            tupleElement(exponentialTimeDecayedCount(500)(time), 'value') AS expected_count
        FROM
        (
            SELECT value, time
            FROM source
            ORDER BY first_order
        )
    ),
    second_direct AS
    (
        SELECT
            tupleElement(exponentialTimeDecayedSum(500)(value, time), 'value') AS reordered_sum,
            exponentialTimeDecayedAvg(500)(value, time) AS reordered_avg,
            tupleElement(exponentialTimeDecayedCount(500)(time), 'value') AS reordered_count
        FROM
        (
            SELECT value, time
            FROM source
            ORDER BY second_order
        )
    ),
    batch_states AS
    (
        SELECT
            batch_count,
            batch_key % batch_count AS batch_id,
            exponentialTimeDecayedSumState(500)(value, time) AS sum_state,
            exponentialTimeDecayedAvgState(500)(value, time) AS avg_state,
            exponentialTimeDecayedCountState(500)(time) AS count_state
        FROM source
        CROSS JOIN
        (
            SELECT arrayJoin([1, 5, 31, 257, 2048]) AS batch_count
        )
        GROUP BY
            batch_count,
            batch_id
    ),
    merged AS
    (
        SELECT
            batch_count,
            tupleElement(exponentialTimeDecayedSumMerge(500)(sum_state), 'value') AS actual_sum,
            exponentialTimeDecayedAvgMerge(500)(avg_state) AS actual_avg,
            tupleElement(exponentialTimeDecayedCountMerge(500)(count_state), 'value') AS actual_count
        FROM batch_states
        GROUP BY batch_count
    )
SELECT
    batch_count,
    abs(reordered_sum - expected_sum) <= 1e-12 * greatest(1., abs(expected_sum)),
    abs(reordered_avg - expected_avg) <= 1e-12 * greatest(1., abs(expected_avg)),
    abs(reordered_count - expected_count) <= 1e-12 * greatest(1., abs(expected_count)),
    abs(actual_sum - expected_sum) <= 1e-12 * greatest(1., abs(expected_sum)),
    abs(actual_avg - expected_avg) <= 1e-12 * greatest(1., abs(expected_avg)),
    abs(actual_count - expected_count) <= 1e-12 * greatest(1., abs(expected_count))
FROM merged
CROSS JOIN first_direct
CROSS JOIN second_direct
ORDER BY batch_count;
