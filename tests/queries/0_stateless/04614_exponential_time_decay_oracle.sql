DROP TABLE IF EXISTS time_decay_feature_gate;
DROP TABLE IF EXISTS time_decay_feature_gate_blocked;

SET allow_experimental_time_decay_aggregate_functions = 1;

CREATE TABLE time_decay_feature_gate
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory;

INSERT INTO time_decay_feature_gate VALUES ((1, 0, 10));
DETACH TABLE time_decay_feature_gate;

SET allow_experimental_time_decay_aggregate_functions = 0;

-- Existing metadata must remain attachable for recovery, but new CREATE and
-- ALTER operations cannot persist the experimental type without opting in.
ATTACH TABLE time_decay_feature_gate;
CREATE TABLE time_decay_feature_gate_blocked
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE time_decay_feature_gate
    ADD COLUMN blocked ExponentialTimeDecayingFloat64(10); -- { serverError ILLEGAL_COLUMN }

-- All scalar operations on the experimental value type remain gated.
SELECT exponentialTimeDecayingValueAt(value, toFloat64(1)) FROM time_decay_feature_gate; -- { serverError UNKNOWN_FUNCTION }
SELECT exponentialTimeDecayingDecayLength(value) FROM time_decay_feature_gate; -- { serverError UNKNOWN_FUNCTION }
SELECT exponentialTimeDecayingAdd(value, value) FROM time_decay_feature_gate; -- { serverError UNKNOWN_FUNCTION }
SELECT value + value FROM time_decay_feature_gate; -- { serverError UNKNOWN_FUNCTION }

-- The legacy OVER form remains available without the experimental setting and
-- keeps the window-only properties used by analyzer rewrites.
SET enable_analyzer = 0;

SELECT
    toTypeName(decayed_sum),
    toTypeName(decayed_avg),
    round(decayed_sum, 6),
    round(decayed_avg, 6)
FROM
(
    SELECT
        exponentialTimeDecayedSum(10)(value, time)
            OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS decayed_sum,
        exponentialTimeDecayedAvg(10)(value, time)
            OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS decayed_avg
    FROM VALUES('value Float64, time Float64', (8, 0))
)
SETTINGS aggregate_functions_null_for_empty = 1;

-- The inner order is semantically significant because the legacy window form
-- evaluates relative to the last row in the frame. It must not be removed as a
-- duplicate of the outer ORDER BY.
SELECT
    time,
    round(
        exponentialTimeDecayedSum(10)(value, time)
            OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        6)
FROM
(
    SELECT *
    FROM VALUES('value Float64, time Float64', (8, 0), (4, 10))
    ORDER BY time DESC
)
ORDER BY time;

-- Exercise the corresponding QueryAnalyzer rewrite as well.
SET enable_analyzer = 1;

SELECT
    toTypeName(decayed_sum),
    toTypeName(decayed_avg),
    round(decayed_sum, 6),
    round(decayed_avg, 6)
FROM
(
    SELECT
        exponentialTimeDecayedSum(10)(value, time)
            OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS decayed_sum,
        exponentialTimeDecayedAvg(10)(value, time)
            OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS decayed_avg
    FROM VALUES('value Float64, time Float64', (8, 0))
)
SETTINGS aggregate_functions_null_for_empty = 1;

DROP TABLE time_decay_feature_gate;

SET allow_experimental_time_decay_aggregate_functions = 1;

-- Compare the aggregate implementation against an independent expression of
-- the defining formula. This detects errors shared by direct and merge paths.
WITH
    source AS
    (
        SELECT
            toFloat64((sipHash64(number, 701) % 100000) + 1) / 1000 AS value,
            toFloat64(sipHash64(number, 702) % 100000) / 100 AS time,
            sipHash64(number, 703) % 37 AS batch
        FROM numbers(1024)
    ),
    anchor AS
    (
        SELECT max(time) AS max_time
        FROM source
    ),
    expected AS
    (
        SELECT
            sum(value * exp((time - max_time) / 37)) AS weighted_sum,
            sum(exp((time - max_time) / 37)) AS weight,
            max_time
        FROM source
        CROSS JOIN anchor
        GROUP BY max_time
    ),
    direct AS
    (
        SELECT
            exponentialTimeDecayedSum(37)(value, time) AS decaying_sum,
            exponentialTimeDecayedAvg(37)(value, time) AS weighted_avg,
            exponentialTimeDecayedCount(37)(time) AS decaying_count
        FROM source
    ),
    batch_states AS
    (
        SELECT
            batch,
            exponentialTimeDecayedSumState(37)(value, time) AS sum_state,
            exponentialTimeDecayedAvgState(37)(value, time) AS avg_state,
            exponentialTimeDecayedCountState(37)(time) AS count_state
        FROM source
        GROUP BY batch
    ),
    merged AS
    (
        SELECT
            exponentialTimeDecayedSumMerge(37)(sum_state) AS decaying_sum,
            exponentialTimeDecayedAvgMerge(37)(avg_state) AS weighted_avg,
            exponentialTimeDecayedCountMerge(37)(count_state) AS decaying_count
        FROM batch_states
    )
SELECT
    abs(tupleElement(direct.decaying_sum, 'value') - expected.weighted_sum)
        <= 1e-12 * greatest(1., abs(expected.weighted_sum)),
    abs(direct.weighted_avg - expected.weighted_sum / expected.weight)
        <= 1e-12 * greatest(1., abs(expected.weighted_sum / expected.weight)),
    abs(tupleElement(direct.decaying_count, 'value') - expected.weight)
        <= 1e-12 * greatest(1., abs(expected.weight)),
    abs(tupleElement(merged.decaying_sum, 'value') - expected.weighted_sum)
        <= 1e-12 * greatest(1., abs(expected.weighted_sum)),
    abs(merged.weighted_avg - expected.weighted_sum / expected.weight)
        <= 1e-12 * greatest(1., abs(expected.weighted_sum / expected.weight)),
    abs(tupleElement(merged.decaying_count, 'value') - expected.weight)
        <= 1e-12 * greatest(1., abs(expected.weight)),
    tupleElement(direct.decaying_sum, 'time') = expected.max_time,
    tupleElement(merged.decaying_sum, 'time') = expected.max_time
FROM expected
CROSS JOIN direct
CROSS JOIN merged;

-- DateTime64 and Decimal timestamps preserve fractional seconds.
WITH
    numeric AS
    (
        SELECT
            exponentialTimeDecayedSum(0.5)(value, time) AS decaying_sum,
            exponentialTimeDecayedAvg(0.5)(value, time) AS weighted_avg,
            exponentialTimeDecayedCount(0.5)(time) AS decaying_count
        FROM VALUES(
            'value Float64, time Float64',
            (8, 1577836800.125),
            (4, 1577836800.875))
    ),
    datetime64 AS
    (
        SELECT
            exponentialTimeDecayedSum(0.5)(value, time) AS decaying_sum,
            exponentialTimeDecayedAvg(0.5)(value, time) AS weighted_avg,
            exponentialTimeDecayedCount(0.5)(time) AS decaying_count
        FROM VALUES(
            'value Float64, time DateTime64(3, \'UTC\')',
            (8, '2020-01-01 00:00:00.125'),
            (4, '2020-01-01 00:00:00.875'))
    ),
    decimal AS
    (
        SELECT
            exponentialTimeDecayedSum(0.5)(value, time) AS decaying_sum,
            exponentialTimeDecayedAvg(0.5)(value, time) AS weighted_avg,
            exponentialTimeDecayedCount(0.5)(time) AS decaying_count
        FROM VALUES(
            'value Float64, time Decimal64(3)',
            (8, 0.125),
            (4, 0.875))
    )
SELECT
    abs(tupleElement(datetime64.decaying_sum, 'value') - tupleElement(numeric.decaying_sum, 'value')) < 1e-12,
    abs(datetime64.weighted_avg - numeric.weighted_avg) < 1e-12,
    abs(tupleElement(datetime64.decaying_count, 'value') - tupleElement(numeric.decaying_count, 'value')) < 1e-12,
    abs(tupleElement(decimal.decaying_sum, 'value') - tupleElement(numeric.decaying_sum, 'value')) < 1e-12,
    abs(decimal.weighted_avg - numeric.weighted_avg) < 1e-12,
    abs(tupleElement(decimal.decaying_count, 'value') - tupleElement(numeric.decaying_count, 'value')) < 1e-12,
    tupleElement(datetime64.decaying_sum, 'time') = 1577836800.875,
    tupleElement(decimal.decaying_sum, 'time') = 0.875
FROM numeric
CROSS JOIN datetime64
CROSS JOIN decimal;

-- The constructor aggregates multiple rows and is equivalent to the sum form.
SELECT
    toTypeName(constructed) = 'ExponentialTimeDecayingFloat64(10)',
    abs(tupleElement(constructed, 'value') - tupleElement(decaying_sum, 'value')) < 1e-12,
    tupleElement(constructed, 'time') = tupleElement(decaying_sum, 'time'),
    exponentialTimeDecayingDecayLength(constructed) = 10
FROM
(
    SELECT
        exponentialTimeDecayingFloat64(10)(value, time) AS constructed,
        exponentialTimeDecayedSum(10)(value, time) AS decaying_sum
    FROM VALUES('value Float64, time Float64', (8, 0), (4, 10), (2, 5))
);

-- Exercise vector-vector addition rather than only constant expressions.
-- Emit the complete input and result only when a comparison fails so the
-- reference stays quiet on success while CI contains enough data to diagnose
-- a numerical or anchoring error.
SELECT
    id,
    av,
    at,
    bv,
    bt,
    expected_value,
    tupleElement(operator_result, 'value') AS operator_value,
    tupleElement(function_result, 'value') AS function_value,
    operator_value - expected_value AS value_error,
    latest_time AS expected_time,
    tupleElement(operator_result, 'time') AS operator_time,
    tupleElement(function_result, 'time') AS function_time,
    toTypeName(operator_result) AS result_type
FROM
(
    SELECT
        id,
        av,
        at,
        bv,
        bt,
        a + b AS operator_result,
        exponentialTimeDecayingAdd(a, b) AS function_result,
        greatest(at, bt) AS latest_time,
        av * exp((at - latest_time) / 10) + bv * exp((bt - latest_time) / 10) AS expected_value
    FROM
    (
        SELECT
            id,
            av,
            at,
            bv,
            bt,
            CAST((av, at, toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)') AS a,
            CAST((bv, bt, toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)') AS b
        FROM VALUES(
            'id UInt8, av Float64, at Float64, bv Float64, bt Float64',
            (1, 8, 0, 4, 10),
            (2, 2, 10, 4, 10),
            (3, -8, 10, 4, 0))
    )
)
WHERE NOT
(
    abs(tupleElement(operator_result, 'value') - expected_value)
        <= 1e-12 * greatest(1., abs(expected_value))
    AND tupleElement(operator_result, 'time') = latest_time
    AND abs(tupleElement(operator_result, 'value') - tupleElement(function_result, 'value'))
        <= 1e-12 * greatest(1., abs(tupleElement(function_result, 'value')))
    AND tupleElement(operator_result, 'time') = tupleElement(function_result, 'time')
    AND toTypeName(operator_result) = 'ExponentialTimeDecayingFloat64(10)'
)
ORDER BY id;

SELECT 'vector addition oracle completed';

-- Finalized values support implicit and explicit decay lengths, plus State/Merge.
WITH
    source AS
    (
        SELECT
            id,
            CAST((value, time, toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)') AS decaying_value
        FROM VALUES(
            'id UInt8, value Float64, time Float64',
            (1, 8, 0),
            (2, 4, 10),
            (3, 2, 5),
            (4, 1, 7))
    ),
    direct AS
    (
        SELECT
            exponentialTimeDecayedSum(decaying_value) AS implicit_result,
            exponentialTimeDecayedSum(10)(decaying_value) AS explicit_result
        FROM source
    ),
    states AS
    (
        SELECT
            id % 2 AS batch,
            exponentialTimeDecayedSumState(decaying_value) AS state
        FROM source
        GROUP BY batch
    ),
    merged AS
    (
        SELECT exponentialTimeDecayedSumMerge(state) AS result
        FROM states
    )
SELECT
    abs(tupleElement(implicit_result, 'value') - tupleElement(explicit_result, 'value')) < 1e-12,
    tupleElement(implicit_result, 'time') = tupleElement(explicit_result, 'time'),
    abs(tupleElement(implicit_result, 'value') - tupleElement(result, 'value')) < 1e-12,
    tupleElement(implicit_result, 'time') = tupleElement(result, 'time'),
    exponentialTimeDecayingDecayLength(implicit_result) = 10,
    exponentialTimeDecayingDecayLength(result) = 10
FROM direct
CROSS JOIN merged;

-- A non-integral decay length survives type parsing, storage, and AMT merging.
DROP TABLE IF EXISTS exponential_time_decay_non_integral;
CREATE TABLE exponential_time_decay_non_integral
(
    key UInt8,
    value SimpleAggregateFunction(
        exponentialTimeDecayedSum,
        ExponentialTimeDecayingFloat64(0.1))
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO exponential_time_decay_non_integral
SELECT 1, exponentialTimeDecayingFloat64(0.1)(2, toFloat64(0));
INSERT INTO exponential_time_decay_non_integral
SELECT 1, exponentialTimeDecayingFloat64(0.1)(0, toFloat64(0.1));

OPTIMIZE TABLE exponential_time_decay_non_integral FINAL;

SELECT
    toTypeName(value),
    abs(tupleElement(value, 'value') - 2 / exp(1)) < 1e-12,
    abs(tupleElement(value, 'time') - 0.1) < 1e-12,
    exponentialTimeDecayingDecayLength(value) = 0.1
FROM exponential_time_decay_non_integral;

DROP TABLE exponential_time_decay_non_integral;

-- Reject invalid argument values and malformed or incompatible state.
SELECT exponentialTimeDecayedSum(10)(toFloat64('nan'), toFloat64(0)); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedSum(10)(toFloat64(1), toFloat64('inf')); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayingValueAt(
    CAST((toFloat64(1), toFloat64(0), toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)'),
    toFloat64('-inf')); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayingAdd(
    CAST((toFloat64(1), toFloat64(0), toFloat64(20)), 'ExponentialTimeDecayingFloat64(10)'),
    CAST((toFloat64(1), toFloat64(0), toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)')); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedSum(decaying_value)
FROM
(
    SELECT CAST(
        (toFloat64(1), toFloat64(0), toFloat64(20)),
        'ExponentialTimeDecayingFloat64(10)') AS decaying_value
); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedSum(toFloat64(1), toFloat64(0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT exponentialTimeDecayedSum(10, 20)(toFloat64(1), toFloat64(0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT CAST((toFloat64(1), toFloat64(0), toFloat64(-1)), 'ExponentialTimeDecayingFloat64(-1)'); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedSumMerge(20)(state)
FROM
(
    SELECT exponentialTimeDecayedSumState(10)(toFloat64(1), toFloat64(0)) AS state
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
