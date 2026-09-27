SET allow_experimental_time_decay_aggregate_functions = 1;

-- The constructor is a scalar function, not an aggregate-function alias.
SELECT name, is_aggregate
FROM system.functions
WHERE name = 'exponentialTimeDecaying';

-- Vector input preserves one constructed value per source row. Include signed,
-- zero, and microsecond DateTime64 values so this cannot pass through constant
-- folding or aggregate collapse.
SELECT
    id,
    toTypeName(decaying_value),
    exponentialTimeDecayingDecayLength(decaying_value),
    abs(exponentialTimeDecayingValueAt(decaying_value, occurred_at) - value) < 1e-12
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
SELECT
    count(exponentialTimeDecaying(3)(value, occurred_at)),
    abs(
        sum(exponentialTimeDecayingValueAt(
            exponentialTimeDecaying(3)(value, occurred_at),
            occurred_at))
        - sum(value)) < 1e-12
FROM values(
    'value Float64, occurred_at DateTime64(6, \'UTC\')',
    (0.5, '2026-09-27 12:00:00.000001'),
    (-0.25, '2026-09-27 12:00:00.500003'),
    (0, '2026-09-27 12:00:00.750007'));

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

-- Wrapping each raw row in a finalized value before aggregation must be
-- equivalent to aggregating the raw (value, time) rows directly.
WITH toDateTime64('2026-09-27 12:00:01.250011', 6, 'UTC') AS target_time
SELECT
    abs(
        exponentialTimeDecayingValueAt(raw_result, target_time)
        - exponentialTimeDecayingValueAt(explicit_wrapped_result, target_time)
    ) < 1e-12,
    abs(
        exponentialTimeDecayingValueAt(raw_result, target_time)
        - exponentialTimeDecayingValueAt(inferred_wrapped_result, target_time)
    ) < 1e-12
FROM
(
    SELECT
        exponentialTimeDecayedSum(3)(value, occurred_at) AS raw_result,
        exponentialTimeDecayedSum(3)(
            exponentialTimeDecaying(3)(value, occurred_at)
        ) AS explicit_wrapped_result,
        exponentialTimeDecayedSum(
            exponentialTimeDecaying(3)(value, occurred_at)
        ) AS inferred_wrapped_result
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
    activity AggregateFunction(
        exponentialTimeDecayedSum(3),
        ExponentialTimeDecaying(3))
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
    ) AS activity
FROM exponential_time_decay_nested_state_source
GROUP BY key;

INSERT INTO exponential_time_decay_nested_state_source VALUES
    (1, 0.75, '2026-09-27 12:00:01.000009'),
    (2, -2, '2026-09-27 12:00:00.750007');

INSERT INTO exponential_time_decay_nested_state_source VALUES
    (1, 0.5, '2026-09-27 12:00:00.000001'),
    (2, 1, '2026-09-27 12:00:01.500013');

INSERT INTO exponential_time_decay_nested_state_source VALUES
    (1, -0.25, '2026-09-27 12:00:00.500003'),
    (2, 0, '2026-09-27 12:00:00.250002');

OPTIMIZE TABLE exponential_time_decay_nested_state_destination FINAL;

-- Compare the persisted, merged finalized-value states against direct aggregation
-- of the raw source rows. This simultaneously checks MV execution, DateTime64(6)
-- handling, signed/zero values, out-of-order batches, and storage-engine merging.
WITH toDateTime64('2026-09-27 12:00:02.000017', 6, 'UTC') AS target_time
SELECT
    actual.key,
    abs(actual.value_at_target - expected.value_at_target)
        <= 1e-12 * greatest(1., abs(expected.value_at_target))
FROM
(
    SELECT
        key,
        exponentialTimeDecayingValueAt(
            exponentialTimeDecayedSumMerge(3)(activity),
            target_time) AS value_at_target
    FROM exponential_time_decay_nested_state_destination
    GROUP BY key
) AS actual
INNER JOIN
(
    SELECT
        key,
        exponentialTimeDecayingValueAt(
            exponentialTimeDecayedSum(3)(value, occurred_at),
            target_time) AS value_at_target
    FROM exponential_time_decay_nested_state_source
    GROUP BY key
) AS expected USING (key)
ORDER BY actual.key;

DROP VIEW exponential_time_decay_nested_state_mv;
DROP TABLE exponential_time_decay_nested_state_source;
DROP TABLE exponential_time_decay_nested_state_destination;
