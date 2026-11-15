-- Gini coefficient aggregate function.

-- Perfect equality: all values the same.
SELECT gini(x) FROM (SELECT 100 AS x FROM numbers(10));

-- Even spacing over 0..9.
SELECT gini(number) FROM numbers(10);

-- Perfect inequality for n=4: one non-zero value.
SELECT gini(x) FROM (SELECT number = 3 AS x FROM numbers(4));

-- GROUP BY: two groups, equal and unequal.
SELECT g, abs(gini(x) - if(g = 0, 7.0 / 30, 1.0 / 5)) < 1e-15 FROM (
    SELECT number % 2 AS g, 5 AS x FROM numbers(10)
    UNION ALL
    SELECT number % 2 AS g, number AS x FROM numbers(10)
) GROUP BY g ORDER BY g;

-- Fewer than 2 values -> NaN.
SELECT gini(1);
SELECT gini(x) FROM (SELECT 1 AS x) WHERE 0;

-- Sum of values is 0 -> NaN.
SELECT gini(x) FROM (SELECT 0 AS x FROM numbers(11));

-- Large finite Float64 values do not overflow the accumulation.
SELECT gini(x) FROM (SELECT [1e308, 1e308] :: Array(Float64) AS arr) ARRAY JOIN arr AS x;

-- Integer differences beyond Float64 precision are preserved.
SELECT gini(x) FROM (
    SELECT [toUInt64(9007199254740992), toUInt64(9007199254740993)] AS arr
) ARRAY JOIN arr AS x;

-- Adjacent wide integer and decimal values remain distinct during finalization.
SELECT gini(x) > 0 FROM (
    SELECT [bitShiftLeft(toUInt128(1), 120), bitShiftLeft(toUInt128(1), 120) + toUInt128(1)] AS arr
) ARRAY JOIN arr AS x;
SELECT gini(x) > 0 FROM (
    SELECT [bitShiftLeft(toUInt256(1), 240), bitShiftLeft(toUInt256(1), 240) + toUInt256(1)] AS arr
) ARRAY JOIN arr AS x;
SELECT gini(x) > 0 FROM (
    SELECT [toDecimal128('1000000000000000000000000000000', 0), toDecimal128('1000000000000000000000000000001', 0)] AS arr
) ARRAY JOIN arr AS x;
SELECT gini(x) > 0 FROM (
    SELECT [
        toDecimal256('1000000000000000000000000000000000000000000000000000000000000', 0),
        toDecimal256('1000000000000000000000000000000000000000000000000000000000001', 0)
    ] AS arr
) ARRAY JOIN arr AS x;

-- Negative values are rejected.
SELECT gini(x) FROM (SELECT [10, 20, -5] :: Array(Int32) AS arr) ARRAY JOIN arr AS x; -- { serverError BAD_ARGUMENTS }

-- Infinite values are rejected.
SELECT gini(x) FROM (SELECT [inf, inf] :: Array(Float64) AS arr) ARRAY JOIN arr AS x; -- { serverError BAD_ARGUMENTS }

-- NaN inputs are skipped.
SELECT gini(x) FROM (SELECT [1, 2, nan, 4] :: Array(Float64) AS arr) ARRAY JOIN arr AS x;

-- BFloat16 NaNs are skipped, infinities are rejected, and differences are widened before subtraction.
SELECT gini(x) FROM (SELECT [1, nan, 3] :: Array(BFloat16) AS arr) ARRAY JOIN arr AS x;
SELECT gini(x) FROM (SELECT [inf, inf] :: Array(BFloat16) AS arr) ARRAY JOIN arr AS x; -- { serverError BAD_ARGUMENTS }
SELECT abs(gini(x) - 1.0 / 514) < 1e-15 FROM (SELECT [1, 1.0078125] :: Array(BFloat16) AS arr) ARRAY JOIN arr AS x;

-- Over a Nullable column the result is Nullable, NULLs are skipped, and a single
-- remaining value still gives NaN. The plain form and the wrapped forms agree, as they do for sum.
SELECT toTypeName(gini(x)), gini(x) FROM (SELECT [1, NULL, 3] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;
SELECT toTypeName(gini(x)), gini(x) FROM (SELECT [1, NULL] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;
SELECT toTypeName(giniIf(x, x > 1)), giniIf(x, x > 1) FROM (SELECT [1, NULL, 3] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;
SELECT toTypeName(giniDistinct(x)), giniDistinct(x) FROM (SELECT [1, NULL, 3] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;

-- With no non-NULL value the result is NULL, for the plain form as well as the wrapped ones.
SELECT toTypeName(gini(x)), isNull(gini(x)) FROM (SELECT [NULL, NULL] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;
SELECT toTypeName(giniIf(x, x > 100)), isNull(giniIf(x, x > 100)) FROM (SELECT [1, NULL, 3] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;

-- A literal NULL argument folds to Nullable(Nothing), as it does for sum.
SELECT toTypeName(gini(NULL)), isNull(gini(NULL));

-- Combinators that keep the argument shape see the fold; Array, ForEach and Map reject it first, as for sum.
SELECT toTypeName(giniIf(NULL, 1)), isNull(giniIf(NULL, 1));
SELECT giniMerge(state) FROM (SELECT giniState(NULL) AS state); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT giniIfMerge(state) FROM (SELECT giniIfState(NULL, NULL) AS state); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(giniDistinct(NULL)), isNull(giniDistinct(NULL));
SELECT toTypeName(giniArgMin(NULL, 1)), isNull(giniArgMin(NULL, 1));
SELECT toTypeName(giniArgMax(NULL, 1)), isNull(giniArgMax(NULL, 1));
SELECT toTypeName(giniArgMin(1, NULL)), isNull(giniArgMin(1, NULL));
SELECT toTypeName(giniOrDefault(NULL)), isNull(giniOrDefault(NULL));
SELECT toTypeName(giniOrNull(NULL)), isNull(giniOrNull(NULL));
WITH giniResample(0, 2, 1)(NULL, 0) AS result
SELECT toTypeName(result), isNull(result);

-- Under aggregate_functions_null_for_empty the -OrNull rewrite applies on empty input, as it does for sum.
SELECT toTypeName(gini(number)), isNull(gini(number)) FROM numbers(0) SETTINGS aggregate_functions_null_for_empty = 1;
SELECT toTypeName(giniIf(number, number > 100)), isNull(giniIf(number, number > 100)) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1;
SELECT toTypeName(giniMerge(state)), isNull(giniMerge(state)) FROM (SELECT giniState(number) AS state FROM numbers(0)) SETTINGS aggregate_functions_null_for_empty = 1;

-- -State / -Merge round trip: split values into two states and merge.
SELECT giniMerge(state) FROM (
    SELECT giniState(number) AS state FROM numbers(5)
    UNION ALL
    SELECT giniState(number + 5) AS state FROM numbers(5)
);

-- -If combinator.
SELECT giniIf(number, number < 5) FROM numbers(10);

-- -Array combinator.
SELECT abs(giniArray(arr) - 7.0 / 16) < 1e-15 FROM (SELECT [1, 2, 3, 10] :: Array(Int32) AS arr);
