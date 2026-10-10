SET allow_experimental_time_decay_aggregate_functions = 1;

-- The constructor is a scalar function, not an aggregate-function alias.
SELECT name, is_aggregate
FROM system.functions
WHERE name = 'exponentialTimeDecaying';

-- Vector input preserves one constructed value per source row. Include signed,
-- zero, and microsecond DateTime64 values so this cannot pass through constant
-- folding or aggregate collapse. The independent numeric oracle clears the low
-- bit of each positive unit timestamp, then evaluates the key-defined curve.
-- Raw 128-bit construction separately retains strict source-value fidelity.
WITH
    unit_time -> reinterpretAsFloat64(bitAnd(
        reinterpretAsUInt64(unit_time), toUInt64(18446744073709551614))) AS quantize_time,
    (raw_value, raw_time, target_time) -> if(raw_value = 0, 0.,
        sign(raw_value) * exp((quantize_time(raw_time + 3 * log(abs(raw_value))) - target_time) / 3)) AS key_value_at
SELECT
    id,
    toTypeName(decaying_value),
    exponentialTimeDecayingDecayLength(decaying_value),
    abs(exponentialTimeDecayingValueAt(decaying_value, occurred_at)
        - key_value_at(value, toFloat64(occurred_at), toFloat64(occurred_at))) < 1e-12,
    abs(exponentialTimeDecayingValueAt(
        CAST((value, occurred_at), 'ExponentialTimeDecaying128(3)'), occurred_at) - value) < 1e-12
FROM
(
    SELECT
        id,
        value,
        occurred_at,
        exponentialTimeDecaying(3)(value, occurred_at) AS decaying_value
    FROM values(
        'id UInt8, value Float64, occurred_at DateTime64(6, \'UTC\')',
        (1, 0.5, '2026-09-27 12:00:00.000001'),
        (2, -0.25, '2026-09-27 12:00:00.500003'),
        (3, 0, '2026-09-27 12:00:00.750007'))
)
ORDER BY id;

-- Ordinary aggregates may consume expressions containing the constructor.
-- This catches fixes that merely whitelist one specific nested aggregate.
WITH
    unit_time -> reinterpretAsFloat64(bitAnd(
        reinterpretAsUInt64(unit_time), toUInt64(18446744073709551614))) AS quantize_time,
    (raw_value, raw_time, target_time) -> if(raw_value = 0, 0.,
        sign(raw_value) * exp((quantize_time(raw_time + 3 * log(abs(raw_value))) - target_time) / 3)) AS key_value_at
SELECT
    count(exponentialTimeDecaying(3)(value, occurred_at)),
    abs(
        sum(exponentialTimeDecayingValueAt(
            exponentialTimeDecaying(3)(value, occurred_at),
            occurred_at))
        - sum(key_value_at(value, toFloat64(occurred_at), toFloat64(occurred_at)))) < 1e-12,
    abs(
        sum(exponentialTimeDecayingValueAt(
            CAST((value, occurred_at), 'ExponentialTimeDecaying128(3)'), occurred_at))
        - sum(value)) < 1e-12
FROM values(
    'value Float64, occurred_at DateTime64(6, \'UTC\')',
    (0.5, '2026-09-27 12:00:00.000001'),
    (-0.25, '2026-09-27 12:00:00.500003'),
    (0, '2026-09-27 12:00:00.750007'));

-- Scalar execution must handle ColumnConst on either argument without collapsing
-- the vector side or losing timestamp precision.
WITH
    toDateTime64('2026-09-27 12:00:00.125001', 6, 'UTC') AS constant_time,
    unit_time -> reinterpretAsFloat64(bitAnd(
        reinterpretAsUInt64(unit_time), toUInt64(18446744073709551614))) AS quantize_time,
    (raw_value, raw_time, target_time) -> if(raw_value = 0, 0.,
        sign(raw_value) * exp((quantize_time(raw_time + 3 * log(abs(raw_value))) - target_time) / 3)) AS key_value_at
SELECT
    abs(
        sum(exponentialTimeDecayingValueAt(
            exponentialTimeDecaying(3)(value, constant_time),
            constant_time))
        - sum(value)) < 1e-12,
    abs(
        sum(exponentialTimeDecayingValueAt(
            exponentialTimeDecaying(3)(toFloat64(2), occurred_at),
            occurred_at))
        - sum(key_value_at(2., toFloat64(occurred_at), toFloat64(occurred_at)))) < 1e-12,
    abs(
        sum(exponentialTimeDecayingValueAt(
            CAST((toFloat64(2), occurred_at), 'ExponentialTimeDecaying128(3)'), occurred_at))
        - 6) < 1e-12
FROM values(
    'value Float64, occurred_at DateTime64(6, \'UTC\')',
    (0.5, '2026-09-27 12:00:00.000001'),
    (-0.25, '2026-09-27 12:00:00.500003'),
    (0, '2026-09-27 12:00:00.750007'));

-- Scalarization must preserve canonical-value validation.
SELECT exponentialTimeDecaying(3)(toFloat64('nan'), toFloat64(0)); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecaying(3)(toFloat64(1), toFloat64('inf')); -- { serverError BAD_ARGUMENTS }

-- Explicit and inferred decay-length forms both build an aggregate state over
-- the scalar constructor and retain their distinct AggregateFunction signatures.
SELECT
    toTypeName(explicit_state),
    toTypeName(inferred_state)
FROM
(
    SELECT
        exponentialTimeDecayedSumState(3)(
            exponentialTimeDecaying(3)(value, occurred_at)
        ) AS explicit_state,
        exponentialTimeDecayedSumState(
            exponentialTimeDecaying(3)(value, occurred_at)
        ) AS inferred_state
    FROM values(
        'value Float64, occurred_at DateTime64(6, \'UTC\')',
        (0.5, '2026-09-27 12:00:00.000001'),
        (-0.25, '2026-09-27 12:00:00.500003'),
        (0, '2026-09-27 12:00:00.750007'))
);

-- The 64-bit aggregate consumes quantized input curves and quantizes its final
-- result. Build that oracle from raw numeric pairs, without a decaying value.
-- Raw 128-bit casts separately preserve the raw-input equivalence assertion.
WITH
    unit_time -> reinterpretAsFloat64(bitAnd(
        reinterpretAsUInt64(unit_time), toUInt64(18446744073709551614))) AS quantize_time,
    (raw_value, raw_time, target_time) -> if(raw_value = 0, 0.,
        sign(raw_value) * exp((quantize_time(raw_time + 3 * log(abs(raw_value))) - target_time) / 3)) AS key_value_at
SELECT
    abs(
        key_value_at(key_sum, key_anchor, toFloat64(anchor_time))
        - exponentialTimeDecayingValueAt(explicit_wrapped_result, anchor_time)
    ) < 1e-12,
    abs(
        key_value_at(key_sum, key_anchor, toFloat64(anchor_time))
        - exponentialTimeDecayingValueAt(inferred_wrapped_result, anchor_time)
    ) < 1e-12,
    abs(raw_result - exponentialTimeDecayingValueAt(precise_wrapped_result, anchor_time)) < 1e-12
FROM
(
    SELECT
        exponentialTimeDecayedSum(3)(value, occurred_at) AS raw_result,
        exponentialTimeDecayedSum(3)(
            sign(value),
            if(value = 0, 0., quantize_time(toFloat64(occurred_at) + 3 * log(abs(value))))
        ) AS key_sum,
        maxIf(quantize_time(toFloat64(occurred_at) + 3 * log(abs(value))), value != 0) AS key_anchor,
        exponentialTimeDecayedSum(3)(
            CAST((value, occurred_at), 'ExponentialTimeDecaying128(3)')
        ) AS precise_wrapped_result,
        exponentialTimeDecayedSum(3)(
            exponentialTimeDecaying(3)(value, occurred_at)
        ) AS explicit_wrapped_result,
        exponentialTimeDecayedSum(
            exponentialTimeDecaying(3)(value, occurred_at)
        ) AS inferred_wrapped_result,
        max(occurred_at) AS anchor_time
    FROM values(
        'value Float64, occurred_at DateTime64(6, \'UTC\')',
        (0.5, '2026-09-27 12:00:00.000001'),
        (-0.25, '2026-09-27 12:00:00.500003'),
        (0, '2026-09-27 12:00:00.750007'))
);

-- An explicit aggregate decay length must still agree with the constructor type.
SELECT exponentialTimeDecayedSumState(3)(
    exponentialTimeDecaying(4)(value, occurred_at)
)
FROM values(
    'value Float64, occurred_at DateTime64(6, \'UTC\')',
    (0.5, '2026-09-27 12:00:00.000001')); -- { serverError BAD_ARGUMENTS }

DROP VIEW IF EXISTS exponential_time_decay_nested_state_mv;
DROP TABLE IF EXISTS exponential_time_decay_nested_state_source;
DROP TABLE IF EXISTS exponential_time_decay_nested_state_destination;
DROP TABLE IF EXISTS exponential_time_decay_finalized_values;

CREATE TABLE exponential_time_decay_nested_state_source
(
    key UInt8,
    value Float64,
    occurred_at DateTime64(6, 'UTC')
)
ENGINE = Memory;

CREATE TABLE exponential_time_decay_nested_state_destination
(
    key UInt8,
    exhaustion AggregateFunction(
        exponentialTimeDecayedSum(3),
        ExponentialTimeDecaying64(3))
)
ENGINE = AggregatingMergeTree
ORDER BY key;

-- The insert-triggered MV constructs one finalized value per row, then builds
-- one state per key. Multiple INSERTs intentionally arrive out of timestamp order
-- so the destination must merge independently produced states correctly.
CREATE MATERIALIZED VIEW exponential_time_decay_nested_state_mv
TO exponential_time_decay_nested_state_destination
AS
SELECT
    key,
    exponentialTimeDecayedSumState(3)(
        exponentialTimeDecaying(3)(value, occurred_at)
    ) AS exhaustion
FROM exponential_time_decay_nested_state_source
GROUP BY key;

INSERT INTO exponential_time_decay_nested_state_source VALUES
    (1, 0.75, '2026-09-27 12:00:01.000009'),
    (1, 0.125, '2026-09-27 12:00:01.000009'),
    (2, -2, '2026-09-27 12:00:00.750007'),
    (2, 0.5, '2026-09-27 12:00:00.750007');

INSERT INTO exponential_time_decay_nested_state_source VALUES
    (1, 0.5, '2026-09-27 12:00:00.000001'),
    (2, 1, '2026-09-27 12:00:01.500013');

INSERT INTO exponential_time_decay_nested_state_source VALUES
    (1, -0.25, '2026-09-27 12:00:00.500003'),
    (2, 0, '2026-09-27 12:00:00.250002');

-- Persist finalized values separately so the following tests exercise a real
-- ExponentialTimeDecaying64 column rather than constructor nesting.
CREATE TABLE exponential_time_decay_finalized_values
(
    key UInt8,
    batch UInt8,
    exhaustion ExponentialTimeDecaying64(3)
)
ENGINE = Memory;

INSERT INTO exponential_time_decay_finalized_values
SELECT
    key,
    toUInt8(value >= 0) AS batch,
    exponentialTimeDecaying(3)(value, occurred_at) AS exhaustion
FROM exponential_time_decay_nested_state_source;

-- Aggregate parameters are inferred from a qualified finalized-value column.
-- This is the exact production shape without an explicit decay parameter.
WITH
    toDateTime64('2026-09-27 12:00:02.000017', 6, 'UTC') AS target_time,
    aggregated AS
    (
        SELECT
            t.key,
            exponentialTimeDecayedSum(t.exhaustion) AS exhaustion,
            exponentialTimeDecayedSum(3)(t.exhaustion) AS explicit_exhaustion
        FROM exponential_time_decay_finalized_values AS t
        GROUP BY t.key
    )
SELECT
    key,
    toTypeName(exhaustion),
    exponentialTimeDecayingDecayLength(exhaustion),
    abs(
        exponentialTimeDecayingValueAt(exhaustion, target_time)
        - exponentialTimeDecayingValueAt(explicit_exhaustion, target_time)
    ) < 1e-12
FROM aggregated
ORDER BY key;

-- Type inference for the aggregate must survive a subquery alias boundary too.
WITH
    toDateTime64('2026-09-27 12:00:02.000017', 6, 'UTC') AS target_time,
    implicit AS
    (
        SELECT
            t.key,
            exponentialTimeDecayedSum(t.exhaustion) AS exhaustion
        FROM
        (
            SELECT key, exhaustion
            FROM exponential_time_decay_finalized_values
        ) AS t
        GROUP BY t.key
    ),
    expected AS
    (
        SELECT
            t.key,
            exponentialTimeDecayedSum(3)(t.exhaustion) AS exhaustion
        FROM exponential_time_decay_finalized_values AS t
        GROUP BY t.key
    )
SELECT
    implicit.key,
    abs(
        exponentialTimeDecayingValueAt(implicit.exhaustion, target_time)
        - exponentialTimeDecayingValueAt(expected.exhaustion, target_time)
    ) < 1e-12
FROM implicit
INNER JOIN expected USING (key)
ORDER BY implicit.key;

-- The State combinator has the same inference requirement. Produce independent
-- states per key/batch using the exact qualified-column form, then merge them.
WITH
    states AS
    (
        SELECT
            t.key,
            t.batch,
            exponentialTimeDecayedSumState(t.exhaustion) AS exhaustion,
            exponentialTimeDecayedSumState(3)(t.exhaustion) AS explicit_exhaustion
        FROM exponential_time_decay_finalized_values AS t
        GROUP BY
            t.key,
            t.batch
    )
SELECT
    key,
    batch,
    toTypeName(exhaustion),
    toTypeName(explicit_exhaustion)
FROM states
ORDER BY
    key,
    batch;

WITH
    toDateTime64('2026-09-27 12:00:02.000017', 6, 'UTC') AS target_time,
    states AS
    (
        SELECT
            t.key,
            t.batch,
            exponentialTimeDecayedSumState(t.exhaustion) AS exhaustion,
            exponentialTimeDecayedSumState(3)(t.exhaustion) AS explicit_exhaustion
        FROM exponential_time_decay_finalized_values AS t
        GROUP BY
            t.key,
            t.batch
    ),
    merged AS
    (
        SELECT
            s.key,
            exponentialTimeDecayedSumMerge(s.exhaustion) AS exhaustion,
            exponentialTimeDecayedSumMerge(3)(s.explicit_exhaustion) AS explicit_exhaustion
        FROM states AS s
        GROUP BY s.key
    ),
    direct AS
    (
        SELECT
            t.key,
            exponentialTimeDecayedSum(t.exhaustion) AS exhaustion
        FROM exponential_time_decay_finalized_values AS t
        GROUP BY t.key
    )
SELECT
    merged.key,
    abs(
        exponentialTimeDecayingValueAt(merged.exhaustion, target_time)
        - exponentialTimeDecayingValueAt(merged.explicit_exhaustion, target_time)
    ) <= 1e-12 * greatest(
        1.,
        abs(exponentialTimeDecayingValueAt(merged.explicit_exhaustion, target_time))),
    abs(
        exponentialTimeDecayingValueAt(merged.exhaustion, target_time)
        - exponentialTimeDecayingValueAt(direct.exhaustion, target_time)
    ) <= 1e-12 * greatest(
        1.,
        abs(exponentialTimeDecayingValueAt(direct.exhaustion, target_time)))
FROM merged
INNER JOIN direct USING (key)
ORDER BY merged.key;

OPTIMIZE TABLE exponential_time_decay_nested_state_destination FINAL;

-- Merge combinator parameters can be recovered from a qualified persisted state.
-- This is the production query shape: no explicit decay parameter is supplied
-- in SQL; it must come from AggregateFunction(exponentialTimeDecayedSum(3),
-- ExponentialTimeDecaying64(3)).
WITH
    toDateTime64('2026-09-27 12:00:02.000017', 6, 'UTC') AS target_time,
    merged AS
    (
        SELECT
            t.key,
            exponentialTimeDecayedSumMerge(t.exhaustion) AS exhaustion,
            exponentialTimeDecayedSumMerge(3)(t.exhaustion) AS explicit_exhaustion
        FROM exponential_time_decay_nested_state_destination AS t
        GROUP BY t.key
    )
SELECT
    key,
    toTypeName(exhaustion),
    exponentialTimeDecayingDecayLength(exhaustion),
    abs(
        exponentialTimeDecayingValueAt(exhaustion, target_time)
        - exponentialTimeDecayingValueAt(explicit_exhaustion, target_time)
    ) < 1e-12
FROM merged
ORDER BY key;

-- Preserve the AggregateFunction type through a subquery boundary and recover
-- the same implicit Merge parameters from a qualified alias there as well.
WITH
    unit_time -> reinterpretAsFloat64(bitAnd(
        reinterpretAsUInt64(unit_time), toUInt64(18446744073709551614))) AS quantize_time,
    (raw_value, raw_time, target_time) -> if(raw_value = 0, 0.,
        sign(raw_value) * exp((quantize_time(raw_time + 3 * log(abs(raw_value))) - target_time) / 3)) AS key_value_at,
    merged AS
    (
        SELECT
            t.key,
            exponentialTimeDecayedSumMerge(t.exhaustion) AS exhaustion
        FROM
        (
            SELECT
                key,
                exhaustion
            FROM exponential_time_decay_nested_state_destination
        ) AS t
        GROUP BY t.key
    ),
    expected AS
    (
        SELECT
            key,
            exponentialTimeDecayedSum(3)(
                sign(value),
                if(value = 0, 0., quantize_time(toFloat64(occurred_at) + 3 * log(abs(value))))
            ) AS key_sum,
            maxIf(quantize_time(toFloat64(occurred_at) + 3 * log(abs(value))), value != 0) AS key_anchor,
            max(occurred_at) AS anchor_time,
            key_value_at(key_sum, key_anchor, toFloat64(anchor_time)) AS exhaustion
        FROM exponential_time_decay_nested_state_source
        GROUP BY key
    )
SELECT
    merged.key,
    abs(
        exponentialTimeDecayingValueAt(merged.exhaustion, expected.anchor_time)
        - expected.exhaustion
    ) <= 1e-12 * greatest(1., abs(expected.exhaustion))
FROM merged
INNER JOIN expected USING (key)
ORDER BY merged.key;

-- Compare the persisted, merged finalized-value states against independent
-- key-defined aggregation at each raw aggregate anchor. This checks MV
-- execution, DateTime64(6) handling, signed/zero values, out-of-order batches,
-- and storage-engine merging. Raw 128-bit construction keeps raw fidelity strict.
WITH
    unit_time -> reinterpretAsFloat64(bitAnd(
        reinterpretAsUInt64(unit_time), toUInt64(18446744073709551614))) AS quantize_time,
    (raw_value, raw_time, target_time) -> if(raw_value = 0, 0.,
        sign(raw_value) * exp((quantize_time(raw_time + 3 * log(abs(raw_value))) - target_time) / 3)) AS key_value_at
SELECT
    actual.key,
    abs(
        exponentialTimeDecayingValueAt(actual.value, expected.anchor_time)
        - expected.value_at_anchor)
        <= 1e-12 * greatest(1., abs(expected.value_at_anchor)),
    abs(exponentialTimeDecayingValueAt(expected.precise_value, expected.anchor_time)
        - expected.raw_value_at_anchor)
        <= 1e-12 * greatest(1., abs(expected.raw_value_at_anchor))
FROM
(
    SELECT
        key,
        exponentialTimeDecayedSumMerge(3)(exhaustion) AS value
    FROM exponential_time_decay_nested_state_destination
    GROUP BY key
) AS actual
INNER JOIN
(
    SELECT
        key,
        exponentialTimeDecayedSum(3)(
            sign(value),
            if(value = 0, 0., quantize_time(toFloat64(occurred_at) + 3 * log(abs(value))))
        ) AS key_sum,
        maxIf(quantize_time(toFloat64(occurred_at) + 3 * log(abs(value))), value != 0) AS key_anchor,
        max(occurred_at) AS anchor_time,
        key_value_at(key_sum, key_anchor, toFloat64(anchor_time)) AS value_at_anchor,
        exponentialTimeDecayedSum(3)(value, occurred_at) AS raw_value_at_anchor,
        exponentialTimeDecayedSum(3)(
            CAST((value, occurred_at), 'ExponentialTimeDecaying128(3)')
        ) AS precise_value
    FROM exponential_time_decay_nested_state_source
    GROUP BY key
) AS expected USING (key)
ORDER BY actual.key;

DROP VIEW exponential_time_decay_nested_state_mv;
DROP TABLE exponential_time_decay_nested_state_source;
DROP TABLE exponential_time_decay_nested_state_destination;
DROP TABLE exponential_time_decay_finalized_values;
