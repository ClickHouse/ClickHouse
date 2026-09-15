SET allow_experimental_time_decay_aggregate_functions = 1;
SET exponential_time_decay_aggregate_function_calculation_budget = 5;

-- The constructor alias accepts indexed values too. Persisted states must keep
-- their small contributions even when the query opts into approximate merging.
DROP TABLE IF EXISTS time_decay_reconstruction;
CREATE TABLE time_decay_reconstruction
(
    key UInt8,
    state AggregateFunction(exponentialTimeDecayingFloat64, ExponentialTimeDecayingFloat64(10))
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO time_decay_reconstruction
SELECT 1, exponentialTimeDecayingFloat64State(CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)'));
INSERT INTO time_decay_reconstruction
SELECT 1, exponentialTimeDecayingFloat64State(CAST((1., 100., 10.), 'ExponentialTimeDecayingFloat64(10)'));

OPTIMIZE TABLE time_decay_reconstruction FINAL;
SELECT round(exponentialTimeDecayingValueAt(finalizeAggregation(state), 100.), 6)
FROM time_decay_reconstruction;
DROP TABLE time_decay_reconstruction;

-- Ordinary query execution still honors the explicitly requested cutoff.
SELECT round(exponentialTimeDecayingValueAt(exponentialTimeDecayingFloat64(value), 100.), 6)
FROM
(
    SELECT CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)') AS value
    UNION ALL
    SELECT CAST((1., 100., 10.), 'ExponentialTimeDecayingFloat64(10)') AS value
);

-- Reconstructing stored type names must not consult an unrelated query budget,
-- even when that budget would be rejected for a new aggregate invocation.
SET exponential_time_decay_aggregate_function_calculation_budget = -1;
SELECT toTypeName(defaultValueOfTypeName('AggregateFunction(exponentialTimeDecayingFloat64(10), Float64, Float64)'));
SELECT toTypeName(defaultValueOfTypeName('AggregateFunction(exponentialTimeDecayedCount(10), Float64)'));
SELECT toTypeName(defaultValueOfTypeName('AggregateFunction(exponentialTimeDecayedAvg(10), Float64, Float64)'));
SELECT exponentialTimeDecayingFloat64(10)(1., 0.); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedCount(10)(0.); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedAvg(10)(1., 0.); -- { serverError BAD_ARGUMENTS }
