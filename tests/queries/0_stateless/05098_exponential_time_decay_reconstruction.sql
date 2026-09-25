SET allow_experimental_time_decay_aggregate_functions = 1;
SET exponential_time_decay_significance_cutoff = 5;

-- The sum aggregate accepts finalized values directly. Persisted states must keep
-- their small contributions even when the query enables the finalized-value cutoff.
DROP TABLE IF EXISTS time_decay_reconstruction;
CREATE TABLE time_decay_reconstruction
(
    key UInt8,
    state AggregateFunction(exponentialTimeDecayedSum, ExponentialTimeDecaying(10))
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO time_decay_reconstruction
SELECT 1, exponentialTimeDecayedSumState(CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)'));
INSERT INTO time_decay_reconstruction
SELECT 1, exponentialTimeDecayedSumState(CAST((1., 100., 10.), 'ExponentialTimeDecaying(10)'));

OPTIMIZE TABLE time_decay_reconstruction FINAL;
SELECT round(exponentialTimeDecayingValueAt(finalizeAggregation(state), 100.), 6)
FROM time_decay_reconstruction;
DROP TABLE time_decay_reconstruction;

-- Ordinary query execution still honors the explicitly requested cutoff.
-- Keep both rows in one source block so this specifically exercises the
-- finalized-value add path. Aggregate-state merges remain exact by design.
SELECT round(exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(value), 100.), 6)
FROM VALUES(
    'value ExponentialTimeDecaying(10)',
    ((1., 0., 10.)),
    ((1., 100., 10.)));

-- Reconstructing stored type names must not consult an unrelated query cutoff,
-- even when that cutoff value would be rejected for a new aggregate invocation.
SET exponential_time_decay_significance_cutoff = -1;
SELECT toTypeName(defaultValueOfTypeName('AggregateFunction(exponentialTimeDecaying(10), Float64, Float64)'));
SELECT toTypeName(defaultValueOfTypeName('AggregateFunction(exponentialTimeDecayedCount(10), Float64)'));
SELECT toTypeName(defaultValueOfTypeName('AggregateFunction(exponentialTimeDecayedAvg(10), Float64, Float64)'));
SELECT exponentialTimeDecaying(10)(1., 0.); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedCount(10)(0.); -- { serverError BAD_ARGUMENTS }
SELECT exponentialTimeDecayedAvg(10)(1., 0.); -- { serverError BAD_ARGUMENTS }
