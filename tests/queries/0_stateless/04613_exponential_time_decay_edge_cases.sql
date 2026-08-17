SET allow_experimental_time_decay_aggregate_functions = 1;

-- Empty aggregates use the canonical zero representation.
SELECT
    tupleElement(decaying_sum, 'sign'),
    tupleElement(decaying_sum, 'signed_unit_time') = 0,
    exponentialTimeDecayingDecayLength(decaying_sum),
    isNaN(decaying_avg),
    tupleElement(decaying_count, 'sign'),
    tupleElement(decaying_count, 'signed_unit_time') = 0,
    exponentialTimeDecayingDecayLength(decaying_count)
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
    round(exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(10)(value, time), toFloat64(10)), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(exponentialTimeDecayingValueAt(exponentialTimeDecayedCount(10)(time), toFloat64(10)), 6)
FROM VALUES(
    'value Nullable(Float64), time Nullable(Float64)',
    (2, 0),
    (NULL, 10),
    (4, NULL),
    (6, 10));

-- A sufficiently old contribution underflows to zero without producing a
-- non-finite result.
SELECT
    exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(1)(value, time), toFloat64(0)),
    exponentialTimeDecayedAvg(1)(value, time),
    exponentialTimeDecayingValueAt(exponentialTimeDecayedCount(1)(time), toFloat64(0))
FROM VALUES('value Float64, time Float64', (1000, -10000), (2, 0));

WITH
    exponentialTimeDecayingFloat64(1)(1000, toFloat64(-10000)) AS old_value,
    exponentialTimeDecayingFloat64(1)(2, toFloat64(0)) AS current_value,
    old_value + current_value AS combined
SELECT
    exponentialTimeDecayingValueAt(combined, toFloat64(0)),
    toFloat64(0),
    round(exponentialTimeDecayingValueAt(combined, toFloat64(1)), 6);
