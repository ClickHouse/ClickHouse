-- Test type-dispatch error paths in several aggregate functions.
--
-- AggregateFunctionSum.cpp (src/AggregateFunctions/AggregateFunctionSum.cpp:67-68):
-- createAggregateFunctionSum throws ILLEGAL_TYPE_OF_ARGUMENT when the argument
-- type is neither numeric nor Decimal. The stateless suite never passes a
-- String, Array, or UUID column to sum(), so this branch is never exercised.
-- Without it, a bad call to sum() on a non-numeric column would produce an
-- obscure or missing error message instead of a clear type error.
--
-- AggregateFunctionQuantileExactExclusive.cpp (lines 40-41, 43-44):
-- createAggregateFunctionQuantile dispatches on the argument type. Date and
-- DateTime types branch at lines 40-41 but no existing test calls
-- quantileExactExclusive() on a Date or DateTime column. An illegal type
-- (e.g. String) falls through to the throw at lines 43-44, also never tested.
--
-- AggregateFunctionQuantileExactInclusive.cpp has the same structure and the
-- same gaps for quantileExactInclusive().

-- sum: ILLEGAL_TYPE_OF_ARGUMENT for non-numeric types (lines 67-68)
SELECT sum(x) FROM (SELECT 'hello' AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sum(x) FROM (SELECT [1, 2, 3] AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- quantileExactExclusive: Date branch (line 40) — returns days-since-epoch as Float64
SELECT quantileExactExclusive(0.5)(toDate('2024-01-15'));

-- quantileExactExclusive: DateTime branch (line 41) — returns Unix timestamp as Float64
SELECT quantileExactExclusive(0.5)(toDateTime('2024-01-15 12:00:00', 'UTC'));

-- quantileExactExclusive: ILLEGAL_TYPE_OF_ARGUMENT for String (lines 43-44)
SELECT quantileExactExclusive(0.5)(x) FROM (SELECT 'hello' AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- quantileExactInclusive: Date branch (same structure)
SELECT quantileExactInclusive(0.5)(toDate('2024-01-15'));

-- quantileExactInclusive: DateTime branch
SELECT quantileExactInclusive(0.5)(toDateTime('2024-01-15 12:00:00', 'UTC'));

-- quantileExactInclusive: ILLEGAL_TYPE_OF_ARGUMENT for String
SELECT quantileExactInclusive(0.5)(x) FROM (SELECT 'hello' AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
