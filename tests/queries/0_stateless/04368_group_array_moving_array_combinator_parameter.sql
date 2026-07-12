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
-- The window-size parameter accepts positive Int64 too. The printed state type name must keep
-- the ::Int64 suffix so it round-trips losslessly (reparsing 42 alone would give UInt64).
SELECT toTypeName(groupArrayMovingSumState(42::Int64)(number)) FROM numbers(1);
SELECT toTypeName(groupArrayMovingAvgState(42::Int64)(number)) FROM numbers(1);
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

-- argMin / argMax (and aliases argAndMin / argAndMax / min_by / max_by) take no parameters
-- either; they silently dropped them, so the -Array combinator later aborted on the
-- wrapper-vs-nested parameter mismatch. They now reject parameters up front too.
SELECT argMinArray('two-sided', 2147483647)([0., 0, 1, 1], [0., 10, 1025, 3]); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
SELECT argMin('two-sided', 1)(number, number) FROM numbers(3); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
SELECT argMaxArray(2)([1, 2, 3], [1, 2, 3]); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
SELECT min_by('two-sided', 1)(number, number) FROM numbers(3); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
-- parameterless argMin/argMax still work (bare and via -Array).
SELECT argMin(number, number) FROM numbers(5);
SELECT argMinArray([1, 2, 3], [3, 2, 1]);

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

-- Same upgrade contract for kolmogorovSmirnovTest: a legacy parameterless
-- AggregateFunction(kolmogorovSmirnovTest, ...) column must stay usable with the parameterized
-- ...Merge(...) and accept aggregate-to-aggregate inserts of new parameterized states.
DROP TABLE IF EXISTS legacy_ks;
CREATE TABLE legacy_ks (s AggregateFunction(kolmogorovSmirnovTest, Float64, Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO legacy_ks SELECT kolmogorovSmirnovTestState(x, y) FROM (SELECT arrayJoin([1., 2, 3, 4]) AS x, arrayJoin([0., 0, 1, 1]) AS y);
SELECT tupleElement(kolmogorovSmirnovTestMerge('two-sided')(s), 'd_statistic') BETWEEN 0 AND 1 FROM legacy_ks;
INSERT INTO legacy_ks SELECT kolmogorovSmirnovTestState('two-sided')(x, y) FROM (SELECT arrayJoin([5., 6]) AS x, arrayJoin([2., 2]) AS y);
SELECT tupleElement(kolmogorovSmirnovTestMerge('two-sided')(s), 'd_statistic') BETWEEN 0 AND 1 FROM legacy_ks;
DROP TABLE legacy_ks;

-- Same upgrade contract for mannWhitneyUTest.
DROP TABLE IF EXISTS legacy_mwu;
CREATE TABLE legacy_mwu (s AggregateFunction(mannWhitneyUTest, Float64, Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO legacy_mwu SELECT mannWhitneyUTestState(x, y) FROM (SELECT arrayJoin([1., 2, 3, 4]) AS x, arrayJoin([0., 0, 1, 1]) AS y);
SELECT tupleElement(mannWhitneyUTestMerge('two-sided', 1)(s), 'u_statistic') >= 0 FROM legacy_mwu;
INSERT INTO legacy_mwu SELECT mannWhitneyUTestState('two-sided', 1)(x, y) FROM (SELECT arrayJoin([5., 6]) AS x, arrayJoin([2., 2]) AS y);
SELECT tupleElement(mannWhitneyUTestMerge('two-sided', 1)(s), 'u_statistic') >= 0 FROM legacy_mwu;
DROP TABLE legacy_mwu;
