DROP TABLE IF EXISTS time_decay_feature_gate;
DROP TABLE IF EXISTS time_decay_feature_gate_blocked;
DROP TABLE IF EXISTS time_decay_default_insert;
DROP TABLE IF EXISTS time_decay_default_alter;
DROP TABLE IF EXISTS time_decay_default_simple_aggregate;
DROP TABLE IF EXISTS time_decay_aggregate_state_reattach;
DROP TABLE IF EXISTS time_decay_simple_aggregate_reattach;
DROP TABLE IF EXISTS time_decay_layout_compatible_insert;
DROP TABLE IF EXISTS time_decay_nested_layout_compatible_insert;
DROP TABLE IF EXISTS time_decay_layout_compatible_simple_aggregate_insert;
DROP TABLE IF EXISTS time_decay_mv_source;
DROP TABLE IF EXISTS time_decay_mv_reattach;

SET allow_experimental_time_decay_aggregate_functions = 1;

-- The implicit default must preserve the decay-length marker encoded in the type.
SELECT
    tupleElement(defaultValueOfTypeName('ExponentialTimeDecayingFloat64(10)'), 'sign') = 0
    AND tupleElement(defaultValueOfTypeName('ExponentialTimeDecayingFloat64(10)'), 'signed_unit_time') = 0
    AND tupleElement(defaultValueOfTypeName('ExponentialTimeDecayingFloat64(10)'), 'decay_length') = 10;

-- Direct typed input uses the validating serialization, including when nested.
SELECT tupleElement(value, 'decay_length') = 10
FROM VALUES('value ExponentialTimeDecayingFloat64(10)', ((1., 0., 10.)));
SELECT tupleElement(values[1], 'decay_length') = 10
FROM VALUES('values Array(ExponentialTimeDecayingFloat64(10))', ([(1., 0., 10.)]));
SELECT *
FROM VALUES('value ExponentialTimeDecayingFloat64(10)', ((1., 0., 20.))); -- { serverError BAD_ARGUMENTS }
SELECT *
FROM VALUES('values Array(ExponentialTimeDecayingFloat64(10))', ([(1., 0., 20.)])); -- { serverError BAD_ARGUMENTS }

CREATE TABLE time_decay_default_insert
(
    id UInt8,
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory;
INSERT INTO time_decay_default_insert (id) VALUES (1);
SELECT tupleElement(value, 'sign') = 0 AND tupleElement(value, 'signed_unit_time') = 0
FROM time_decay_default_insert;

CREATE TABLE time_decay_default_alter (id UInt8) ENGINE = Memory;
INSERT INTO time_decay_default_alter VALUES (1);
ALTER TABLE time_decay_default_alter
    ADD COLUMN value ExponentialTimeDecayingFloat64(10);
SELECT tupleElement(value, 'sign') = 0 AND tupleElement(value, 'signed_unit_time') = 0
FROM time_decay_default_alter;

CREATE TABLE time_decay_default_simple_aggregate
(
    id UInt8,
    value SimpleAggregateFunction(
        exponentialTimeDecayedSum,
        ExponentialTimeDecayingFloat64(10))
)
ENGINE = AggregatingMergeTree
ORDER BY id;
INSERT INTO time_decay_default_simple_aggregate (id) VALUES (1);
SELECT
    tupleElement(value, 'sign') = 0 AND tupleElement(value, 'signed_unit_time') = 0,
    exponentialTimeDecayingDecayLength(value) = 10
FROM time_decay_default_simple_aggregate;

DROP TABLE time_decay_default_insert;
DROP TABLE time_decay_default_alter;
DROP TABLE time_decay_default_simple_aggregate;

-- A layout-compatible named Tuple is not the experimental type. INSERT must
-- still build the conversion so the existing per-row validator runs.
CREATE TABLE time_decay_layout_compatible_insert
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory;
INSERT INTO time_decay_layout_compatible_insert
SELECT CAST(
    (1., 0., 20.),
    'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)'); -- { serverError BAD_ARGUMENTS }
INSERT INTO time_decay_layout_compatible_insert
SELECT CAST(
    (1., 0., 10.),
    'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)');
SELECT count() = 1 FROM time_decay_layout_compatible_insert;
DROP TABLE time_decay_layout_compatible_insert;

CREATE TABLE time_decay_nested_layout_compatible_insert
(
    values Array(ExponentialTimeDecayingFloat64(10))
)
ENGINE = Memory;
INSERT INTO time_decay_nested_layout_compatible_insert
SELECT CAST(
    [(1., 0., 20.)],
    'Array(Tuple(sign Float64, signed_unit_time Float64, decay_length Float64))'); -- { serverError BAD_ARGUMENTS }
INSERT INTO time_decay_nested_layout_compatible_insert
SELECT CAST(
    [(1., 0., 10.)],
    'Array(Tuple(sign Float64, signed_unit_time Float64, decay_length Float64))');
SELECT count() = 1 FROM time_decay_nested_layout_compatible_insert;
DROP TABLE time_decay_nested_layout_compatible_insert;

CREATE TABLE time_decay_layout_compatible_simple_aggregate_insert
(
    id UInt8,
    value SimpleAggregateFunction(
        exponentialTimeDecayedSum,
        ExponentialTimeDecayingFloat64(10))
)
ENGINE = AggregatingMergeTree
ORDER BY id;
INSERT INTO time_decay_layout_compatible_simple_aggregate_insert
SELECT
    1,
    CAST(
        (1., 0., 20.),
        'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)'); -- { serverError BAD_ARGUMENTS }
INSERT INTO time_decay_layout_compatible_simple_aggregate_insert
SELECT
    1,
    CAST(
        (1., 0., 10.),
        'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)');
SELECT count() = 1 FROM time_decay_layout_compatible_simple_aggregate_insert;
DROP TABLE time_decay_layout_compatible_simple_aggregate_insert;

CREATE TABLE time_decay_feature_gate
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory;

INSERT INTO time_decay_feature_gate VALUES ((1, 0, 10));
DETACH TABLE time_decay_feature_gate;

CREATE TABLE time_decay_aggregate_state_reattach
(
    id UInt8,
    value AggregateFunction(exponentialTimeDecayedSum(10), Float64, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;
DETACH TABLE time_decay_aggregate_state_reattach;

CREATE TABLE time_decay_simple_aggregate_reattach
(
    id UInt8,
    value SimpleAggregateFunction(
        exponentialTimeDecayedSum,
        ExponentialTimeDecayingFloat64(10))
)
ENGINE = AggregatingMergeTree
ORDER BY id;
DETACH TABLE time_decay_simple_aggregate_reattach;

CREATE TABLE time_decay_mv_source
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory;

CREATE MATERIALIZED VIEW time_decay_mv_reattach
ENGINE = Memory
AS SELECT value FROM time_decay_mv_source;
DETACH TABLE time_decay_mv_reattach;

SET allow_experimental_time_decay_aggregate_functions = 0;

-- Reconstruction is allowed, but normal aggregate execution remains gated.
SELECT exponentialTimeDecayedSum(10)(toFloat64(1), toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT exponentialTimeDecayedAvg(10)(toFloat64(1), toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT exponentialTimeDecayedCount(10)(toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT exponentialTimeDecayingFloat64(10)(toFloat64(1), toFloat64(0)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

-- Existing metadata must remain attachable for recovery, but new CREATE and
-- ALTER operations cannot persist the experimental type without opting in.
ATTACH TABLE time_decay_feature_gate;
ATTACH TABLE time_decay_aggregate_state_reattach;
ATTACH TABLE time_decay_simple_aggregate_reattach;
ATTACH TABLE time_decay_mv_reattach;
SELECT 'aggregate metadata attach preserved';
SELECT 'simple aggregate metadata attach preserved';
SELECT 'materialized view short attach preserved';
INSERT INTO time_decay_mv_source VALUES ((1, 1, 10));
SELECT count() FROM time_decay_mv_reattach;

-- Full ATTACH definitions are fresh DDL, not metadata recovery, and must obey
-- the same experimental type gate as CREATE.
ATTACH TABLE time_decay_full_attach_blocked
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
ATTACH TABLE time_decay_aggregate_attach_blocked
(
    value AggregateFunction(exponentialTimeDecayedSum(10), Float64, Float64)
)
ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

-- Materialized views use the same schema gate for both inferred CREATE columns
-- and explicit columns in full ATTACH definitions.
CREATE MATERIALIZED VIEW time_decay_mv_create_blocked
ENGINE = Memory
AS SELECT value FROM time_decay_mv_source; -- { serverError ILLEGAL_COLUMN }
ATTACH MATERIALIZED VIEW time_decay_mv_attach_blocked
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory
AS SELECT value FROM time_decay_mv_source; -- { serverError ILLEGAL_COLUMN }

CREATE MATERIALIZED VIEW time_decay_mv_nested_type_blocked
(
    value Array(ExponentialTimeDecayingFloat64(10))
)
ENGINE = Memory
AS SELECT [value] FROM time_decay_mv_source; -- { serverError ILLEGAL_COLUMN }

-- The focused view validation must preserve unrelated experimental type gates.
SET enable_time_time64_type = 0;
CREATE MATERIALIZED VIEW time_decay_mv_time64_blocked
(
    value Time64(3)
)
ENGINE = Memory
AS SELECT now64(3); -- { serverError ILLEGAL_COLUMN }

SET allow_experimental_nullable_tuple_type = 0;
CREATE MATERIALIZED VIEW time_decay_mv_nullable_tuple_blocked
(
    value Nullable(Tuple(n UInt8))
)
ENGINE = Memory
AS SELECT CAST(NULL, 'Nullable(Tuple(n UInt8))') AS value; -- { serverError ILLEGAL_COLUMN }

-- Materialized views historically skip unrelated suspicious-type validation.
-- Adding the time-decay gate must not change that behavior.
CREATE MATERIALIZED VIEW time_decay_mv_unrelated_validation
(
    value LowCardinality(UInt64)
)
ENGINE = Memory
AS SELECT toUInt64(1) AS value;
SELECT 'unrelated materialized view validation preserved';
DROP TABLE time_decay_mv_unrelated_validation;

CREATE TABLE time_decay_feature_gate_blocked
(
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE time_decay_feature_gate
    ADD COLUMN blocked ExponentialTimeDecayingFloat64(10); -- { serverError ILLEGAL_COLUMN }

-- Type reconstruction remains available for expression evaluation and persisted
-- metadata. The setting gates execution and fresh schema persistence.
SELECT toTypeName(_CAST(tuple(1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)'))
    = 'ExponentialTimeDecayingFloat64(10)';

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
DROP TABLE time_decay_aggregate_state_reattach;
DROP TABLE time_decay_simple_aggregate_reattach;
DROP TABLE time_decay_mv_reattach;
DROP TABLE time_decay_mv_source;

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
    abs(exponentialTimeDecayingValueAt(direct.decaying_sum, expected.max_time) - expected.weighted_sum)
        <= 1e-12 * greatest(1., abs(expected.weighted_sum)),
    abs(direct.weighted_avg - expected.weighted_sum / expected.weight)
        <= 1e-12 * greatest(1., abs(expected.weighted_sum / expected.weight)),
    abs(exponentialTimeDecayingValueAt(direct.decaying_count, expected.max_time) - expected.weight)
        <= 1e-12 * greatest(1., abs(expected.weight)),
    abs(exponentialTimeDecayingValueAt(merged.decaying_sum, expected.max_time) - expected.weighted_sum)
        <= 1e-11 * greatest(1., abs(expected.weighted_sum)),
    abs(merged.weighted_avg - expected.weighted_sum / expected.weight)
        <= 1e-12 * greatest(1., abs(expected.weighted_sum / expected.weight)),
    abs(exponentialTimeDecayingValueAt(merged.decaying_count, expected.max_time) - expected.weight)
        <= 1e-11 * greatest(1., abs(expected.weight)),
    isFinite(tupleElement(direct.decaying_sum, 'signed_unit_time')),
    isFinite(tupleElement(merged.decaying_sum, 'signed_unit_time'))
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
    abs(exponentialTimeDecayingValueAt(datetime64.decaying_sum, toFloat64(1577836800.875)) - exponentialTimeDecayingValueAt(numeric.decaying_sum, toFloat64(1577836800.875))) < 1e-12,
    abs(datetime64.weighted_avg - numeric.weighted_avg) < 1e-12,
    abs(exponentialTimeDecayingValueAt(datetime64.decaying_count, toFloat64(1577836800.875)) - exponentialTimeDecayingValueAt(numeric.decaying_count, toFloat64(1577836800.875))) < 1e-12,
    abs(exponentialTimeDecayingValueAt(decimal.decaying_sum, toFloat64(0.875)) - exponentialTimeDecayingValueAt(numeric.decaying_sum, toFloat64(1577836800.875)))
        <= 1e-6 * greatest(1., abs(exponentialTimeDecayingValueAt(numeric.decaying_sum, toFloat64(1577836800.875)))),
    abs(decimal.weighted_avg - numeric.weighted_avg) < 1e-12,
    abs(exponentialTimeDecayingValueAt(decimal.decaying_count, toFloat64(0.875)) - exponentialTimeDecayingValueAt(numeric.decaying_count, toFloat64(1577836800.875)))
        <= 1e-6 * greatest(1., abs(exponentialTimeDecayingValueAt(numeric.decaying_count, toFloat64(1577836800.875)))),
    isFinite(tupleElement(datetime64.decaying_sum, 'signed_unit_time')),
    isFinite(tupleElement(decimal.decaying_sum, 'signed_unit_time'))
FROM numeric
CROSS JOIN datetime64
CROSS JOIN decimal;

-- The constructor aggregates multiple rows and is equivalent to the sum form.
SELECT
    toTypeName(constructed) = 'ExponentialTimeDecayingFloat64(10)',
    abs(exponentialTimeDecayingValueAt(constructed, toFloat64(10)) - exponentialTimeDecayingValueAt(decaying_sum, toFloat64(10))) < 1e-12,
    tupleElement(constructed, 'signed_unit_time') = tupleElement(decaying_sum, 'signed_unit_time'),
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
    exponentialTimeDecayingValueAt(operator_result, latest_time) AS operator_value,
    exponentialTimeDecayingValueAt(function_result, latest_time) AS function_value,
    operator_value - expected_value AS value_error,
    latest_time AS expected_time,
    tupleElement(operator_result, 'signed_unit_time') AS operator_signed_unit_time,
    tupleElement(function_result, 'signed_unit_time') AS function_signed_unit_time,
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
            CAST((toFloat64(sign(av)), toFloat64(sign(av)) * (at + 10 * log(abs(av))), toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)') AS a,
            CAST((toFloat64(sign(bv)), toFloat64(sign(bv)) * (bt + 10 * log(abs(bv))), toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)') AS b
        FROM VALUES(
            'id UInt8, av Float64, at Float64, bv Float64, bt Float64',
            (1, 8, 0, 4, 10),
            (2, 2, 10, 4, 10),
            (3, -8, 10, 4, 0))
    )
)
WHERE NOT
(
    abs(exponentialTimeDecayingValueAt(operator_result, latest_time) - expected_value)
        <= 1e-12 * greatest(1., abs(expected_value))
    AND isFinite(tupleElement(operator_result, 'signed_unit_time'))
    AND abs(exponentialTimeDecayingValueAt(operator_result, latest_time) - exponentialTimeDecayingValueAt(function_result, latest_time))
        <= 1e-12 * greatest(1., abs(exponentialTimeDecayingValueAt(function_result, latest_time)))
    AND tupleElement(operator_result, 'signed_unit_time') = tupleElement(function_result, 'signed_unit_time')
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
            CAST((toFloat64(sign(value)), toFloat64(sign(value)) * (time + 10 * log(abs(value))), toFloat64(10)), 'ExponentialTimeDecayingFloat64(10)') AS decaying_value
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
    abs(exponentialTimeDecayingValueAt(implicit_result, toFloat64(10)) - exponentialTimeDecayingValueAt(explicit_result, toFloat64(10))) < 1e-12,
    tupleElement(implicit_result, 'signed_unit_time') = tupleElement(explicit_result, 'signed_unit_time'),
    abs(exponentialTimeDecayingValueAt(implicit_result, toFloat64(10)) - exponentialTimeDecayingValueAt(result, toFloat64(10))) < 1e-12,
    abs(tupleElement(implicit_result, 'signed_unit_time') - tupleElement(result, 'signed_unit_time'))
        <= 1e-12 * greatest(1., abs(tupleElement(implicit_result, 'signed_unit_time'))),
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
    abs(exponentialTimeDecayingValueAt(value, toFloat64(0.1)) - 2 / exp(1)) < 1e-12,
    isFinite(tupleElement(value, 'signed_unit_time')),
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

-- Malformed reconstructed constants are rejected before hash-key materialization.
SELECT count()
FROM
(
    SELECT _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)') AS value
)
GROUP BY value; -- { serverError BAD_ARGUMENTS }

SELECT DISTINCT _CAST(
    [(1., 0., 20.)],
    'Array(ExponentialTimeDecayingFloat64(10))'); -- { serverError BAD_ARGUMENTS }

-- Generic tuple comparison and sorting must validate every reconstructed row.
WITH
    _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)') AS malformed,
    _CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS valid
SELECT malformed = valid; -- { serverError BAD_ARGUMENTS }

WITH
    _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)') AS malformed,
    _CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS valid
SELECT malformed < valid; -- { serverError BAD_ARGUMENTS }

SELECT value
FROM
(
    SELECT _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)') AS value
    UNION ALL
    SELECT _CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS value
)
ORDER BY value; -- { serverError BAD_ARGUMENTS }

-- Validation at the conversion boundary also protects sorting helpers which
-- invoke the native column comparator directly instead of sortBlock.
SELECT arraySort([
    _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)'),
    _CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)')
]); -- { serverError BAD_ARGUMENTS }

-- Set-backed membership must validate reconstructed values on both sides.
WITH
    _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)') AS malformed,
    _CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS valid
SELECT malformed IN (valid); -- { serverError BAD_ARGUMENTS }

WITH
    _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)') AS malformed,
    _CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS valid
SELECT valid IN (malformed); -- { serverError BAD_ARGUMENTS }

WITH
    _CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(10)') AS malformed,
    _CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS valid
SELECT malformed NOT IN (valid); -- { serverError BAD_ARGUMENTS }

-- Pairwise compatibility is enforced recursively before generic tuple
-- comparison and set hashing.
WITH
    CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecayingFloat64(10))') AS left_value,
    CAST([(1., 0., 20.)], 'Array(ExponentialTimeDecayingFloat64(20))') AS right_value
SELECT left_value = right_value; -- { serverError BAD_ARGUMENTS }

WITH
    CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecayingFloat64(10))') AS decaying,
    CAST([(1., 0., 10.)], 'Array(Tuple(sign Float64, signed_unit_time Float64, decay_length Float64))') AS plain
SELECT decaying = plain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS decaying,
    CAST((1., 0., 10.), 'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS plain
SELECT decaying IN (plain); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH
    CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecayingFloat64(10))') AS decaying,
    CAST([(1., 0., 10.)], 'Array(Tuple(sign Float64, signed_unit_time Float64, decay_length Float64))') AS plain
SELECT decaying IN (plain); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Empty-set shortcuts must retain recursive validation and pairwise compatibility.
WITH _CAST([(1., 0., 20.)], 'Array(ExponentialTimeDecayingFloat64(10))') AS malformed
SELECT malformed IN
(
    SELECT CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecayingFloat64(10))')
    FROM numbers(0)
); -- { serverError BAD_ARGUMENTS }

WITH CAST((1., 0., 10.), 'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS plain
SELECT plain IN
(
    SELECT CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)')
    FROM numbers(0)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH CAST([(1., 0., 10.)], 'Array(Tuple(sign Float64, signed_unit_time Float64, decay_length Float64))') AS plain
SELECT plain NOT IN
(
    SELECT CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecayingFloat64(10))')
    FROM numbers(0)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Compatible direct and nested values must retain normal IN semantics. These
-- queries stay silent on success and expose a reference diff on a false negative.
WITH CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS value
SELECT 'direct decaying IN false negative' WHERE NOT (value IN (value));

WITH CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS value
SELECT 'direct decaying multi-value IN false negative' WHERE NOT (value IN (value, value));

WITH CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecayingFloat64(10))') AS value
SELECT 'nested decaying IN false negative' WHERE NOT (value IN (value));

-- MergeTree set indexes may sort internal blocks without type metadata. Optional
-- custom-type validation must not change the native sorter for ordinary columns.
DROP TABLE IF EXISTS time_decay_set_index_sort;
CREATE TABLE time_decay_set_index_sort (key Int8) ENGINE = MergeTree ORDER BY key;
INSERT INTO time_decay_set_index_sort VALUES (-30), (1);
SELECT count() FROM time_decay_set_index_sort WHERE key IN (-30);
DROP TABLE time_decay_set_index_sort;

-- Regular aggregates use aggregation properties when applying OrNull rewrites.
SET enable_analyzer = 1;
SELECT
    isNull(exponentialTimeDecayedSum(10)(value, time)),
    isNull(exponentialTimeDecayedAvg(10)(value, time))
FROM VALUES('value Float64, time Float64', (1, 1))
WHERE false
SETTINGS aggregate_functions_null_for_empty = 1;

-- Old-analyzer parameterized views must not capture the experimental type while disabled.
SET allow_experimental_time_decay_aggregate_functions = 0;
SET enable_analyzer = 0;
CREATE VIEW time_decay_parameterized_view_gate AS
SELECT tupleElement({value:ExponentialTimeDecayingFloat64(10)}, 1); -- { serverError ILLEGAL_COLUMN }
CREATE VIEW time_decay_nested_parameterized_view_gate AS
SELECT length({values:Array(ExponentialTimeDecayingFloat64(10))}); -- { serverError ILLEGAL_COLUMN }
-- Full ATTACH definitions are rejected before type validation, so they cannot
-- bypass the experimental setting.
ATTACH VIEW time_decay_parameterized_view_attach_gate AS
SELECT tupleElement({value:ExponentialTimeDecayingFloat64(10)}, 1); -- { serverError BAD_ARGUMENTS }

-- Identifier parameters are query syntax, not data types, and must remain valid.
DROP TABLE IF EXISTS time_decay_identifier_source;
DROP VIEW IF EXISTS time_decay_identifier_parameter;
CREATE TABLE time_decay_identifier_source (value UInt8) ENGINE = Memory;
CREATE VIEW time_decay_identifier_parameter (value UInt8) AS
SELECT value FROM {database:Identifier}.time_decay_identifier_source;
SELECT 'identifier parameter preserved';
DROP VIEW time_decay_identifier_parameter;
DROP TABLE time_decay_identifier_source;

SET allow_experimental_time_decay_aggregate_functions = 1;
CREATE VIEW time_decay_parameterized_view_gate AS
SELECT tupleElement({value:ExponentialTimeDecayingFloat64(10)}, 1);
SELECT 'typed parameter preserved';
DROP VIEW time_decay_parameterized_view_gate;

-- A short ATTACH reloads existing metadata and remains available while disabled.
CREATE VIEW time_decay_parameterized_view_reattach AS
SELECT tupleElement({value:ExponentialTimeDecayingFloat64(10)}, 1);
DETACH TABLE time_decay_parameterized_view_reattach;
SET allow_experimental_time_decay_aggregate_functions = 0;
ATTACH TABLE time_decay_parameterized_view_reattach;
SELECT 'short attach preserved';
DROP VIEW time_decay_parameterized_view_reattach;
SET allow_experimental_time_decay_aggregate_functions = 1;

-- The implicit empty value is an identity even for genuine negative timestamps.
DROP TABLE IF EXISTS time_decay_default_identity;
CREATE TABLE time_decay_default_identity
(
    id UInt8,
    value ExponentialTimeDecayingFloat64(10)
)
ENGINE = Memory;
INSERT INTO time_decay_default_identity
SELECT 1, exponentialTimeDecayingFloat64(10)(5., -10.);
INSERT INTO time_decay_default_identity (id) VALUES (2);

WITH
    defaultValueOfTypeName('ExponentialTimeDecayingFloat64(10)') AS empty_value,
    exponentialTimeDecayingFloat64(10)(5., -10.) AS observed_value,
    exponentialTimeDecayingAdd(empty_value, observed_value) AS combined
SELECT
    abs(exponentialTimeDecayingValueAt(combined, toFloat64(-10)) - 5)
        <= 1e-12,
    isFinite(tupleElement(combined, 'signed_unit_time'));

WITH exponentialTimeDecayedSum(value) AS combined
SELECT
    abs(exponentialTimeDecayingValueAt(combined, toFloat64(-10)) - 5)
        <= 1e-12,
    isFinite(tupleElement(combined, 'signed_unit_time'))
FROM time_decay_default_identity;

DROP TABLE time_decay_default_identity;
