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

-- intervalLengthSum takes no parameters; passing some used to be silently dropped, so the
-- -Array combinator later aborted on the wrapper-vs-nested parameter mismatch. It now rejects
-- parameters up front instead.
SELECT intervalLengthSumArray('two-sided', 1)([0., 0, 1, 1], [0., 10, 1025, 3]); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
SELECT intervalLengthSum('two-sided', 1)(0., 10.); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }

-- Backward compatibility: the parameters only affect finalization, not the serialized state,
-- so a legacy parameterless state column (written before this patch) must stay Merge-/CAST-
-- compatible with the parameterized function. getNormalizedStateType() normalizes both to the
-- same representation.
SELECT '-- legacy parameterless state compatibility --';
DROP TABLE IF EXISTS legacy_moving;
CREATE TABLE legacy_moving (s AggregateFunction(groupArrayMovingSum, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO legacy_moving SELECT groupArrayMovingSumState(x) FROM (SELECT arrayJoin([1, 2, 3]) AS x);
-- parameterized -Merge over a legacy parameterless column (was ILLEGAL_TYPE_OF_ARGUMENT before the fix)
SELECT groupArrayMovingSumMerge(2)(s) FROM legacy_moving;
-- insert a new parameterized state into the legacy parameterless column, then merge everything.
-- The concatenation order of the two states is not deterministic, so assert order-independent
-- invariants: the merged moving sum has one element per input value (6) and its last element is
-- the running total of all values (1+2+3+10+20+30 = 66).
INSERT INTO legacy_moving SELECT groupArrayMovingSumState(2)(x) FROM (SELECT arrayJoin([10, 20, 30]) AS x);
SELECT length(r), r[length(r)] FROM (SELECT groupArrayMovingSumMerge(s) AS r FROM legacy_moving);
DROP TABLE legacy_moving;
