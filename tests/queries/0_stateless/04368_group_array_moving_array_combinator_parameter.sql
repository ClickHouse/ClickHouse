-- Several aggregate functions accept parameters but previously dropped them from the
-- aggregate function's parameter set (getParameters() returned an empty array). The -Array
-- combinator carried the original parameters, so its wrapper-vs-nested parameter check
-- failed with a LOGICAL_ERROR (abort in debug/sanitizer builds). The parameters are now
-- preserved, so the parameterised functions also survive the -Array combinator and their
-- state type names keep the parameters.

-- groupArrayMovingSum / groupArrayMovingAvg (window-size parameter).
SELECT groupArrayMovingSumArray(42)([1, 2, 3]);
SELECT groupArrayMovingAvgArray(2)([1, 2, 3, 4]);
SELECT groupArrayMovingSumArray([1, 2, 3]);
SELECT toTypeName(groupArrayMovingSumState(42)(number)) FROM numbers(1);
SELECT toTypeName(groupArrayMovingSumState(number)) FROM numbers(1);
SELECT finalizeAggregation(groupArrayMovingSumMergeState(2)(s))
FROM
(
    SELECT groupArrayMovingSumState(2)(number) AS s FROM numbers(4)
    UNION ALL
    SELECT groupArrayMovingSumState(2)(number) AS s FROM numbers(4)
);

-- kolmogorovSmirnovTest / mannWhitneyUTest (string / string+uint parameters):
-- the -Array combinator no longer aborts, and the state type keeps the parameters.
SELECT tupleElement(kolmogorovSmirnovTestArray('two-sided')([1., 2, 3, 4], [0., 0, 1, 1]), 'd_statistic') >= 0;
SELECT tupleElement(mannWhitneyUTestArray('two-sided', 1)([1., 2, 3, 4], [0., 0, 1, 1]), 'u_statistic') >= 0;
SELECT toTypeName(kolmogorovSmirnovTestState('two-sided')(x, y))
FROM (SELECT 1. AS x, 0. AS y);
SELECT toTypeName(mannWhitneyUTestState('two-sided', 1)(x, y))
FROM (SELECT 1. AS x, 0. AS y);
