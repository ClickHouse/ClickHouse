-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105014
--
-- `quantile_fuse_name_mapping` in `src/Interpreters/GatherFunctionQuantileVisitor.cpp` must list every
-- quantile function family that has a singular and plural variant sharing the same internal aggregate
-- state. When a family is missing from the map, `haveSameStateRepresentationImpl` (in
-- `AggregateFunctionQuantile.h`) treats the singular and plural variants as different states, so storing
-- a `quantileXxxState` and merging it back with `quantilesXxxMerge` (or vice versa) raises
-- `ILLEGAL_TYPE_OF_ARGUMENT`, even though the binary states are identical.
--
-- This test exercises every singular/plural quantile pair via cross-function merge to detect any future
-- regression where a new quantile family is added without updating the map.

-- The original issue reproducer.
SELECT 'quantileExactWeightedInterpolated states identical',
    hex(quantileExactWeightedInterpolatedState(0.5)(number, 1)) = hex(quantilesExactWeightedInterpolatedState(0.5)(number, 1))
FROM numbers(5);

SELECT 'quantileExactWeightedInterpolated same-function merge',
    quantileExactWeightedInterpolatedMerge(state)
FROM (SELECT quantileExactWeightedInterpolatedState(0.5)(number, 1) AS state FROM numbers(5));

SELECT 'quantileExactWeightedInterpolated cross-function merge',
    quantilesExactWeightedInterpolatedMerge(0.5)(state)
FROM (SELECT quantileExactWeightedInterpolatedState(0.5)(number, 1) AS state FROM numbers(5));

-- Two other quantile families that were also missing from the map before the fix.
SELECT 'quantileDD cross-function merge',
    quantilesDDMerge(0.01, 0.5)(state)
FROM (SELECT quantileDDState(0.01, 0.5)(number) AS state FROM numbers(5));

SELECT 'quantilePrometheusHistogram cross-function merge',
    quantilesPrometheusHistogramMerge(0.5)(state)
FROM (SELECT quantilePrometheusHistogramState(0.5)(toFloat64(number), toFloat64(number) + 1) AS state FROM numbers(5));

-- The remaining quantile families that were already correctly mapped, exercised here to guard against
-- accidental removal from the map.
SELECT 'quantile cross-function merge',
    quantilesMerge(0.5)(state)
FROM (SELECT quantileState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileBFloat16 cross-function merge',
    quantilesBFloat16Merge(0.5)(state)
FROM (SELECT quantileBFloat16State(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileBFloat16Weighted cross-function merge',
    quantilesBFloat16WeightedMerge(0.5)(state)
FROM (SELECT quantileBFloat16WeightedState(0.5)(number, 1) AS state FROM numbers(5));

SELECT 'quantileDeterministic cross-function merge',
    quantilesDeterministicMerge(0.5)(state)
FROM (SELECT quantileDeterministicState(0.5)(number, number) AS state FROM numbers(5));

SELECT 'quantileExact cross-function merge',
    quantilesExactMerge(0.5)(state)
FROM (SELECT quantileExactState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileExactExclusive cross-function merge',
    quantilesExactExclusiveMerge(0.5)(state)
FROM (SELECT quantileExactExclusiveState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileExactHigh cross-function merge',
    quantilesExactHighMerge(0.5)(state)
FROM (SELECT quantileExactHighState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileExactInclusive cross-function merge',
    quantilesExactInclusiveMerge(0.5)(state)
FROM (SELECT quantileExactInclusiveState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileExactLow cross-function merge',
    quantilesExactLowMerge(0.5)(state)
FROM (SELECT quantileExactLowState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileExactWeighted cross-function merge',
    quantilesExactWeightedMerge(0.5)(state)
FROM (SELECT quantileExactWeightedState(0.5)(number, 1) AS state FROM numbers(5));

SELECT 'quantileGK cross-function merge',
    quantilesGKMerge(100, 0.5)(state)
FROM (SELECT quantileGKState(100, 0.5)(number) AS state FROM numbers(5));

SELECT 'quantileInterpolatedWeighted cross-function merge',
    quantilesInterpolatedWeightedMerge(0.5)(state)
FROM (SELECT quantileInterpolatedWeightedState(0.5)(number, 1) AS state FROM numbers(5));

SELECT 'quantileTDigest cross-function merge',
    quantilesTDigestMerge(0.5)(state)
FROM (SELECT quantileTDigestState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileTDigestWeighted cross-function merge',
    quantilesTDigestWeightedMerge(0.5)(state)
FROM (SELECT quantileTDigestWeightedState(0.5)(number, 1) AS state FROM numbers(5));

SELECT 'quantileTiming cross-function merge',
    quantilesTimingMerge(0.5)(state)
FROM (SELECT quantileTimingState(0.5)(number) AS state FROM numbers(5));

SELECT 'quantileTimingWeighted cross-function merge',
    quantilesTimingWeightedMerge(0.5)(state)
FROM (SELECT quantileTimingWeightedState(0.5)(number, 1) AS state FROM numbers(5));
