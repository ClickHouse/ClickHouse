SET allow_experimental_time_decay_aggregate_functions = 1;

-- A finalized time-decaying value is constructed row by row and can be fed into an aggregate-state builder.
SELECT toTypeName(
    exponentialTimeDecayedSumState(3)(
        exponentialTimeDecaying(3)(value, occurred_at)
    )
)
FROM values(
    'value Float64, occurred_at DateTime64(6)',
    (0.5, '2026-09-27 12:00:00'),
    (0.75, '2026-09-27 12:00:00')
);

DROP VIEW IF EXISTS exponential_time_decay_nested_state_mv;
DROP TABLE IF EXISTS exponential_time_decay_nested_state_source;
DROP TABLE IF EXISTS exponential_time_decay_nested_state_destination;

CREATE TABLE exponential_time_decay_nested_state_source
(
    value Float64,
    occurred_at DateTime64(6)
)
ENGINE = Memory;

CREATE TABLE exponential_time_decay_nested_state_destination
(
    activity AggregateFunction(
        exponentialTimeDecayedSum(3),
        ExponentialTimeDecaying(3))
)
ENGINE = AggregatingMergeTree
ORDER BY tuple();

-- Insert-triggered materialized views must be able to construct one finalized value per source row
-- before building the aggregate state stored in AggregatingMergeTree.
CREATE MATERIALIZED VIEW exponential_time_decay_nested_state_mv
TO exponential_time_decay_nested_state_destination
AS
SELECT
    exponentialTimeDecayedSumState(3)(
        exponentialTimeDecaying(3)(value, occurred_at)
    ) AS activity
FROM exponential_time_decay_nested_state_source;

INSERT INTO exponential_time_decay_nested_state_source VALUES
    (0.5, '2026-09-27 12:00:00'),
    (0.75, '2026-09-27 12:00:00');

SELECT
    abs(
        exponentialTimeDecayingValueAt(
            exponentialTimeDecayedSumMerge(3)(activity),
            toDateTime64('2026-09-27 12:00:00', 6))
        - 1.25) < 1e-12
FROM exponential_time_decay_nested_state_destination;

DROP VIEW exponential_time_decay_nested_state_mv;
DROP TABLE exponential_time_decay_nested_state_source;
DROP TABLE exponential_time_decay_nested_state_destination;
