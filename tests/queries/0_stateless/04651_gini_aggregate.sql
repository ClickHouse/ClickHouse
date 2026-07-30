-- Gini coefficient aggregate function.

-- Perfect equality: all values the same.
SELECT gini(x) FROM (SELECT 100 AS x FROM numbers(10));

-- Even spacing over 0..9.
SELECT gini(number) FROM numbers(10);

-- Perfect inequality for n=4: one non-zero value.
SELECT gini(x) FROM (SELECT number = 3 AS x FROM numbers(4));

-- GROUP BY: two groups, equal and unequal.
SELECT g, gini(x) FROM (
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

-- Negative values are rejected.
SELECT gini(x) FROM (SELECT [10, 20, -5] :: Array(Int32) AS arr) ARRAY JOIN arr AS x; -- { serverError BAD_ARGUMENTS }

-- NaN inputs are skipped.
SELECT gini(x) FROM (SELECT [1, 2, nan, 4] :: Array(Float64) AS arr) ARRAY JOIN arr AS x;

-- Nullable input.
SELECT gini(x) FROM (SELECT [1, NULL, 3] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;

-- An all-NULL nullable input is treated as no values.
SELECT gini(x) FROM (SELECT [NULL, NULL] :: Array(Nullable(Int32)) AS arr) ARRAY JOIN arr AS x;

-- -State / -Merge round trip: split values into two states and merge.
SELECT giniMerge(state) FROM (
    SELECT giniState(number) AS state FROM numbers(5)
    UNION ALL
    SELECT giniState(number + 5) AS state FROM numbers(5)
);

-- -If combinator.
SELECT giniIf(number, number < 5) FROM numbers(10);

-- -Array combinator.
SELECT giniArray(arr) FROM (SELECT [1, 2, 3, 10] :: Array(Int32) AS arr);
