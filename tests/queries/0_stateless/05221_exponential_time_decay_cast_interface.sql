SET allow_experimental_time_decay_aggregate_functions = 1;

-- Public CAST construction accepts all three raw forms. Use a value other than
-- -1/0/1 so the test cannot accidentally pass through the old logical
-- (sign, signed_unit_time, decay_length) interpretation.
WITH
    toFloat64(8) AS value,
    toFloat64(5.25) AS timestamp,
    toFloat64(3) AS decay_length,
    (value, timestamp, decay_length)::ExponentialTimeDecaying AS inferred,
    (value, timestamp, decay_length)::ExponentialTimeDecaying(3) AS checked,
    (value, timestamp)::ExponentialTimeDecaying(3) AS parameterized
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

-- Constant CAST results are serialized through the Native protocol before the
-- data block is materialized. Keep serialization-info discovery valid for ColumnConst.
SELECT CAST((toFloat64(1), toFloat64(65535), toFloat64(10)), 'ExponentialTimeDecaying(10)');

-- The unparameterized target can infer a non-integral decay length and DateTime64
-- input while still producing one concrete static type.
WITH
    toDateTime64('2026-09-28 06:00:00.123456', 6, 'UTC') AS timestamp,
    toFloat64(0.5) AS decay_length,
    (toFloat64(2.5), timestamp, decay_length)::ExponentialTimeDecaying AS value
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
    (raw_value, timestamp, decay_length)::ExponentialTimeDecaying(3) AS value
SELECT
    abs(exponentialTimeDecayingValueAt(value, timestamp) + 1) < 1e-12,
    abs(exponentialTimeDecayingValueAt(value, toFloat64(123)) + 1) > 0.5;

-- The two-field CAST and scalar constructor are equivalent public constructors.
WITH
    toFloat64(-4.5) AS raw_value,
    toFloat64(17.25) AS timestamp,
    (raw_value, timestamp)::ExponentialTimeDecaying(3) AS cast_value,
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
    (raw_value, timestamp)::ExponentialTimeDecaying(3) AS value
SELECT
    exponentialTimeDecayingDecayLength(value) = 3,
    exponentialTimeDecayingValueAt(value, timestamp) = 0;

-- When both tuple and target specify a decay length, they must match.
WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64(4) AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying(3); -- { serverError BAD_ARGUMENTS }

-- The inferred decay length must be finite and positive.
WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64(0) AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying; -- { serverError BAD_ARGUMENTS }

WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64(-1) AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying; -- { serverError BAD_ARGUMENTS }

WITH
    toFloat64(8) AS raw_value,
    toFloat64(5) AS timestamp,
    toFloat64('nan') AS decay_length
SELECT (raw_value, timestamp, decay_length)::ExponentialTimeDecaying; -- { serverError BAD_ARGUMENTS }

-- A tuple carrying the derived sign/unit-time field names is not a value
-- representation at all. Reject it instead of routing it through any
-- (sign, signed_unit_time, decay_length) parsing logic.
WITH CAST(
    (1., 123., 3.),
    'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS forbidden_tuple
SELECT forbidden_tuple::ExponentialTimeDecaying(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH CAST(
    (1., 123., 3.),
    'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS forbidden_tuple
SELECT forbidden_tuple::ExponentialTimeDecaying; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same rejection applies when the invalid derived-field tuple is nested;
-- container conversion must not reinterpret it as a decaying value.
SELECT CAST(
    [CAST(
        (1., 123., 3.),
        'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)')],
    'Array(ExponentialTimeDecaying(3))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Decay length is part of the static type, so direct casts between different
-- parameterizations are type errors rather than malformed values.
WITH CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS value
SELECT CAST(value, 'ExponentialTimeDecaying(20)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The parameterless spelling is inference-only. A standalone type declaration
-- still needs a concrete decay length because column types are static.
CREATE TEMPORARY TABLE time_decay_unparameterized_type_rejected
(
    value ExponentialTimeDecaying
); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
