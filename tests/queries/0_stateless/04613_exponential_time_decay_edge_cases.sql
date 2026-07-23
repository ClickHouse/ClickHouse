SET allow_experimental_time_decay_aggregate_functions = 1;

-- Empty aggregates use their regular default values. The anchor is zero because
-- no timestamp was observed.
SELECT
    tupleElement(decaying_sum, 'value'),
    tupleElement(decaying_sum, 'time'),
    tupleElement(decaying_sum, 'decay_length'),
    isNaN(decaying_avg),
    tupleElement(decaying_count, 'value'),
    tupleElement(decaying_count, 'time'),
    tupleElement(decaying_count, 'decay_length')
FROM
(
    SELECT
        exponentialTimeDecayedSum(10)(value, time) AS decaying_sum,
        exponentialTimeDecayedAvg(10)(value, time) AS decaying_avg,
        exponentialTimeDecayedCount(10)(time) AS decaying_count
    FROM VALUES('value Float64, time Float64', (1, 1))
    WHERE false
);

-- The OrNull combinator distinguishes an empty aggregate from the regular default.
SELECT
    isNull(exponentialTimeDecayedSumOrNull(10)(value, time)),
    isNull(exponentialTimeDecayedAvgOrNull(10)(value, time)),
    isNull(exponentialTimeDecayedCountOrNull(10)(time))
FROM VALUES('value Float64, time Float64', (1, 1))
WHERE false;

-- Nullable aggregate arguments skip rows containing NULL in an argument used by
-- that aggregate. Count only depends on time, so a NULL value does not skip it.
SELECT
    round(tupleElement(exponentialTimeDecayedSum(10)(value, time), 'value'), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(tupleElement(exponentialTimeDecayedCount(10)(time), 'value'), 6)
FROM VALUES(
    'value Nullable(Float64), time Nullable(Float64)',
    (2, 0),
    (NULL, 10),
    (4, NULL),
    (6, 10));

-- A sufficiently old contribution underflows to zero without producing a
-- non-finite result.
SELECT
    tupleElement(exponentialTimeDecayedSum(1)(value, time), 'value'),
    exponentialTimeDecayedAvg(1)(value, time),
    tupleElement(exponentialTimeDecayedCount(1)(time), 'value')
FROM VALUES('value Float64, time Float64', (1000, -10000), (2, 0));

WITH
    exponentialTimeDecayingFloat64(1000, toFloat64(-10000), 1) AS old_value,
    exponentialTimeDecayingFloat64(2, toFloat64(0), 1) AS current_value,
    old_value + current_value AS combined
SELECT
    tupleElement(combined, 'value'),
    tupleElement(combined, 'time'),
    round(exponentialTimeDecayingValueAt(combined, toFloat64(1)), 6);
