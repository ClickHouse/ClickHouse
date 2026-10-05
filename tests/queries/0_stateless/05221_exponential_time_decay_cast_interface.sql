SET allow_experimental_time_decay_aggregate_functions = 1;

-- Public CAST construction accepts all three raw forms. Use a value other than
-- -1/0/1 so the test cannot accidentally pass through the old logical
-- (sign, signed_unit_time, decay_length) interpretation.
WITH
    toFloat64(8) AS value,
    toFloat64(5.25) AS timestamp,
    toFloat64(3) AS decay_length,
    (value, timestamp, decay_length)::ExponentialTimeDecaying64 AS inferred,
    (value, timestamp, decay_length)::ExponentialTimeDecaying64(3) AS checked,
    (value, timestamp)::ExponentialTimeDecaying64(3) AS parameterized
SELECT
    toTypeName(inferred),
    toTypeName(checked),
    toTypeName(parameterized),
    exponentialTimeDecayingDecayLength(inferred),
    exponentialTimeDecayingDecayLength(checked),
    exponentialTimeDecayingDecayLength(parameterized),
    abs(exponentialTimeDecayingValueAt(inferred, timestamp) - value) < 1e-12,
    abs(exponentialTimeDecayingValueAt(checked, timestamp) - value) < 1e-12,
    abs(exponentialTimeDecayingValueAt(parameterized, timestamp) - value) < 1e-12;

-- The precise type supports the same construction forms but derives a UInt128
-- identity from the higher-precision unit timestamp.
WITH
    toFloat64(8) AS value,
    toFloat64(5.25) AS timestamp,
    toFloat64(3) AS decay_length,
    (value, timestamp, decay_length)::ExponentialTimeDecaying128 AS inferred,
    (value, timestamp, decay_length)::ExponentialTimeDecaying128(3) AS checked,
    (value, timestamp)::ExponentialTimeDecaying128(3) AS parameterized
SELECT
    toTypeName(inferred),
    toTypeName(checked),
    toTypeName(parameterized),
    exponentialTimeDecayingDecayLength(inferred),
    exponentialTimeDecayingDecayLength(checked),
    exponentialTimeDecayingDecayLength(parameterized),
    abs(exponentialTimeDecayingValueAt(inferred, timestamp) - value) < 1e-12,
    abs(exponentialTimeDecayingValueAt(checked, timestamp) - value) < 1e-12,
    abs(exponentialTimeDecayingValueAt(parameterized, timestamp) - value) < 1e-12;

-- Constant `CAST` results are serialized through the `Native` protocol before the
-- data block is materialized. Keep serialization-info discovery valid for `ColumnConst`.
SELECT CAST((toFloat64(1), toFloat64(65535), toFloat64(10)), 'ExponentialTimeDecaying64(10)');

-- The unparameterized target can infer a non-integral decay length and DateTime64
-- input while still producing one concrete static type.
WITH
    toDateTime64('2026-09-28 06:00:00.123456', 6, 'UTC') AS timestamp,
    toFloat64(0.5) AS decay_length,
    (toFloat64(2.5), timestamp, decay_length)::ExponentialTimeDecaying64 AS value
SELECT
    toTypeName(value),
    exponentialTimeDecayingDecayLength(value),
    abs(exponentialTimeDecayingValueAt(value, timestamp) - 2.5) < 1e-12;

-- A negative raw value proves that the first two fields are value/timestamp,
-- not sign/signed-unit-time. Under the old logical tuple interpretation this
-- tuple would anchor the curve at +123 rather than -123.
WITH
    toFloat64(-1) AS raw_value,
    toFloat64(-123) AS timestamp,
    toFloat64(3) AS decay_length,
    (raw_value, timestamp, decay_length)::ExponentialTimeDecaying64(3) AS value
SELECT
    abs(exponentialTimeDecayingValueAt(value, timestamp) + 1) < 1e-12,
    abs(exponentialTimeDecayingValueAt(value, toFloat64(123)) + 1) > 0.5;

-- The two-field CAST and scalar constructor are equivalent public constructors.
WITH
    toFloat64(-4.5) AS raw_value,
    toFloat64(17.25) AS timestamp,
    (raw_value, timestamp)::ExponentialTimeDecaying64(3) AS cast_value,
    exponentialTimeDecaying(3)(raw_value, timestamp) AS function_value
SELECT
    toTypeName(cast_value) = toTypeName(function_value),
    exponentialTimeDecayingDecayLength(cast_value) = exponentialTimeDecayingDecayLength(function_value),
    abs(
        exponentialTimeDecayingValueAt(cast_value, timestamp)
        - exponentialTimeDecayingValueAt(function_value, timestamp)
    ) < 1e-12;

-- Zero remains the canonical empty curve regardless of the supplied observation
-- timestamp, while retaining the target decay length.
WITH
    toFloat64(0) AS raw_value,
    toFloat64(123.5) AS timestamp,
    (raw_value, timestamp)::ExponentialTimeDecaying64(3) AS value
SELECT
    exponentialTimeDecayingDecayLength(value) = 3,
    exponentialTimeDecayingValueAt(value, timestamp) = 0;

-- When both tuple and target specify a decay length, they must match.
WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64(4) AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying64(3); -- { serverError BAD_ARGUMENTS }

-- The inferred decay length must be finite and positive.
WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64(0) AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying64; -- { serverError BAD_ARGUMENTS }

WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64(-1) AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying64; -- { serverError BAD_ARGUMENTS }

WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64('nan') AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying64; -- { serverError BAD_ARGUMENTS }

-- A tuple carrying the derived sign/unit-time field names is not a value
-- representation at all. Reject it instead of routing it through any
-- (sign, signed_unit_time, decay_length) parsing logic.
WITH CAST(
    (1., 123., 3.),
    'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS forbidden_tuple
SELECT forbidden_tuple::ExponentialTimeDecaying64(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH CAST(
    (1., 123., 3.),
    'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS forbidden_tuple
SELECT forbidden_tuple::ExponentialTimeDecaying64; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH CAST(
    (1., 123., 3.),
    'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS forbidden_tuple
SELECT forbidden_tuple::ExponentialTimeDecaying128(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH CAST(
    (1., 123., 3.),
    'Tuple(sign Float64, unit_timestamp Float64, decay_length Float64)') AS forbidden_tuple
SELECT forbidden_tuple::ExponentialTimeDecaying64(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same rejection applies when the invalid derived-field tuple is nested;
-- container conversion must not reinterpret it as a decaying value.
SELECT CAST(
    [CAST(
        (1., 123., 3.),
        'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)')],
    'Array(ExponentialTimeDecaying64(3))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Decay length is part of the static type, so direct casts between different
-- parameterizations are type errors rather than malformed values.
WITH CAST((1., 0., 10.), 'ExponentialTimeDecaying64(10)') AS value
SELECT CAST(value, 'ExponentialTimeDecaying64(20)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Explicit conversion is required to cross the key-width boundary.
WITH
    CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS compact,
    CAST(compact, 'ExponentialTimeDecaying128(10)') AS precise,
    CAST(precise, 'ExponentialTimeDecaying64(10)') AS compact_again
SELECT
    toTypeName(precise),
    toTypeName(compact_again),
    abs(exponentialTimeDecayingValueAt(precise, 0.) - 1.) < 1e-12,
    abs(exponentialTimeDecayingValueAt(compact_again, 0.) - 1.) < 1e-12;

SELECT
    CAST((1., 0.), 'ExponentialTimeDecaying64(10)')
    + CAST((1., 0.), 'ExponentialTimeDecaying128(10)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Aggregation of finalized values preserves the exact input key width.
SELECT
    (
        SELECT toTypeName(exponentialTimeDecayedSum(value))
        FROM
        (
            SELECT CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS value
        )
    ),
    (
        SELECT toTypeName(exponentialTimeDecayedSum(value))
        FROM
        (
            SELECT CAST((1., 0.), 'ExponentialTimeDecaying128(10)') AS value
        )
    );

-- State/Merge combinators preserve the same key width as finalized input.
WITH
    compact_state AS
    (
        SELECT exponentialTimeDecayedSumState(value) AS state
        FROM
        (
            SELECT CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS value
        )
    ),
    precise_state AS
    (
        SELECT exponentialTimeDecayedSumState(value) AS state
        FROM
        (
            SELECT CAST((1., 0.), 'ExponentialTimeDecaying128(10)') AS value
        )
    )
SELECT
    (SELECT toTypeName(exponentialTimeDecayedSumMerge(state)) FROM compact_state),
    (SELECT toTypeName(exponentialTimeDecayedSumMerge(state)) FROM precise_state);

-- Compact is the default, but the query setting can select direct storage
-- for constructors and parameterless inferred casts. Explicit datatype modes win.
SET exponential_time_decay_default_mode = 'direct';

WITH
    exponentialTimeDecaying(3)(toFloat64(8), toFloat64(5.25)) AS constructed,
    (toFloat64(8), toFloat64(5.25), toFloat64(3))::ExponentialTimeDecaying64 AS inferred,
    CAST((toFloat64(8), toFloat64(5.25)), 'ExponentialTimeDecaying64(3)') AS explicit_compact
SELECT
    toTypeName(constructed),
    tupleElement(constructed, 'value'),
    tupleElement(constructed, 'timestamp'),
    tupleElement(constructed, 'decay_length'),
    toTypeName(inferred),
    tupleElement(inferred, 'value'),
    tupleElement(inferred, 'timestamp'),
    toTypeName(explicit_compact),
    abs(tupleElement(explicit_compact, 'value')) = 1,
    exponentialTimeDecayingValueAt(
        explicit_compact,
        tupleElement(explicit_compact, 'timestamp')) = tupleElement(explicit_compact, 'value');

SET exponential_time_decay_default_mode = 'invalid';
SELECT exponentialTimeDecaying(3)(toFloat64(1), toFloat64(0)); -- { serverError BAD_ARGUMENTS }
SET exponential_time_decay_default_mode = 'compact';

-- Compact output is a canonical raw pair reconstructed from the authoritative
-- key. Re-parsing that pair must reproduce the same key at either width.
WITH
    CAST((toFloat64(8), toFloat64(5.25)), 'ExponentialTimeDecaying64(3)') AS compact64,
    CAST((toFloat64(8), toFloat64(5.25)), 'ExponentialTimeDecaying128(3)') AS compact128,
    CAST(
        (tupleElement(compact64, 'value'), tupleElement(compact64, 'timestamp')),
        'ExponentialTimeDecaying64(3)') AS roundtrip64,
    CAST(
        (tupleElement(compact128, 'value'), tupleElement(compact128, 'timestamp')),
        'ExponentialTimeDecaying128(3)') AS roundtrip128
SELECT compact64 = roundtrip64, compact128 = roundtrip128;

-- Very large decay lengths may place the mathematical unit time outside the
-- Float64 timestamp range. Compact encoding must use the finite boundary slice
-- and residual value instead of rejecting the curve.
WITH
    CAST((toFloat64(1e-300), toFloat64(0)), 'ExponentialTimeDecaying128(1e308)') AS p_small,
    CAST((toFloat64(1e-200), toFloat64(0)), 'ExponentialTimeDecaying128(1e308)') AS p_large,
    CAST((toFloat64(-1e-300), toFloat64(0)), 'ExponentialTimeDecaying128(1e308)') AS n_small,
    CAST((toFloat64(-1e-200), toFloat64(0)), 'ExponentialTimeDecaying128(1e308)') AS n_large,
    CAST((toFloat64(0), toFloat64(0)), 'ExponentialTimeDecaying128(1e308)') AS zero
SELECT
    isFinite(tupleElement(p_small, 'value')),
    isFinite(tupleElement(p_small, 'timestamp')),
    p_small < p_large,
    p_large > zero,
    zero > n_small,
    n_large < n_small;

-- The width-specific parameterless spelling is inference-only. A standalone type declaration
-- still needs a concrete decay length because column types are static.
CREATE TEMPORARY TABLE time_decay_unparameterized_type_rejected
(
    value ExponentialTimeDecaying64
); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
