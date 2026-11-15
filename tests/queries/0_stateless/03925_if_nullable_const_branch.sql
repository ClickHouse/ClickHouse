-- Reproduce an exception in the `if` function when Nullable branches are wrapped in ColumnConst.
-- The `executeForNullableThenElse` method didn't detect Nullable columns inside ColumnConst,
-- causing the function to fall through to a non-Nullable code path and return the wrong type.

SELECT if(number % 2, 9223372036854775806, toNullable(-9223372036854775808)) AS res FROM numbers(2) GROUP BY number % 2 WITH TOTALS;
