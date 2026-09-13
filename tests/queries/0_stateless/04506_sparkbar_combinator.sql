-- Tests for the Sparkbar aggregate function combinator.
-- {fn}Sparkbar(width, begin_x, end_x)(x_col, ...) applies the nested aggregate function
-- per x-axis bucket and renders the results as a Unicode sparkbar string.

SELECT 'sumSparkbar with range:';
SELECT sumSparkbar(10, 0, 9)(number, number + 1) FROM numbers(10);

SELECT 'countSparkbar with range:';
SELECT countSparkbar(10, 0, 9)(number) FROM numbers(10);

SELECT 'avgSparkbar with range:';
SELECT avgSparkbar(5, 0, 4)(number, number * 2) FROM numbers(5);

SELECT 'minSparkbar with range:';
SELECT minSparkbar(5, 0, 4)(number, 10 - number) FROM numbers(5);

SELECT 'maxSparkbar with range:';
SELECT maxSparkbar(5, 0, 4)(number, number) FROM numbers(5);

-- Non-positive nested results render as blank bars, matching the existing `sparkbar` function
-- (which also treats values <= 0 as empty). The whole-negative case below produces a string of
-- spaces, and the mixed case renders only the buckets whose nested result is strictly positive.
SELECT 'sumSparkbar all-negative nested results (blank):';
SELECT sumSparkbar(3, 0, 2)(number, -toInt64(number + 1)) FROM numbers(3);

SELECT 'sumSparkbar mixed-sign nested results:';
SELECT sumSparkbar(3, 0, 2)(number, toInt64(number) - 1) FROM numbers(3);

-- Multiple rows per bucket
SELECT 'sumSparkbar multiple rows per bucket:';
SELECT sumSparkbar(5, 0, 4)(number % 5, 1) FROM numbers(20);

-- Date axis
SELECT 'sumSparkbar with Date:';
SELECT sumSparkbar(5, toDate('2020-01-01'), toDate('2020-01-05'))(toDate('2020-01-01') + number, number + 1) FROM numbers(5);

-- Result type is String
SELECT 'result type:';
SELECT toTypeName(countSparkbar(5, 0, 4)(number)) FROM numbers(5);

-- Empty result set
SELECT 'empty result set:';
SELECT countSparkbar(5, 0, 4)(number) FROM numbers(0);

-- All values outside range
SELECT 'all values out of range:';
SELECT sumSparkbar(5, 100, 104)(number, 1) FROM numbers(10);

-- Works inside GROUP BY
SELECT 'with GROUP BY:';
SELECT number % 2 AS grp, sumSparkbar(5, 0, 4)(number DIV 2, number)
FROM numbers(10)
GROUP BY grp
ORDER BY grp;

-- uniqSparkbar
SELECT 'uniqSparkbar with range:';
SELECT uniqSparkbar(5, 0, 4)(number, number % 3) FROM numbers(15);

-- DateTime64 x-axis (scale=0): parameters arrive as raw tick counts
SELECT 'countSparkbar with DateTime64 scale=0:';
SELECT countSparkbar(5, toDateTime64('2024-01-01 00:00:00', 0), toDateTime64('2024-01-05 00:00:00', 0))(
    toDateTime64('2024-01-01 00:00:00', 0) + INTERVAL (number) DAY
) FROM numbers(5);

-- DateTime64 x-axis (scale=3): ticks are milliseconds; begin_x/end_x must not be divided by scale
SELECT 'countSparkbar with DateTime64 scale=3:';
SELECT countSparkbar(5, toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-05 00:00:00', 3))(
    toDateTime64('2024-01-01 00:00:00', 3) + INTERVAL (number) DAY
) FROM numbers(5);

-- DateTime64 cross-epoch range (begin_x < 0, end_x > 0): negative ticks must stay signed
SELECT 'countSparkbar with DateTime64 cross-epoch:';
SELECT countSparkbar(5, toDateTime64('1969-12-31 23:59:58', 3), toDateTime64('1970-01-01 00:00:02', 3))(
    toDateTime64('1969-12-31 23:59:58', 3) + INTERVAL (number * 1000) MILLISECOND
) FROM numbers(5);

-- DateTime64 scale mismatch: column scale=6 (μs), parameters scale=3 (ms)
SELECT 'countSparkbar with DateTime64 scale mismatch (col=6, params=3):';
SELECT countSparkbar(5, toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-05 00:00:00', 3))(
    toDateTime64('2024-01-01 00:00:00', 6) + INTERVAL (number) DAY
) FROM numbers(5);

-- DateTime64 column coarser than the bounds (col=0, params=3): bucketing happens at the
-- finest scale (ms), so the fractional bounds are honoured exactly. The range
-- [00:00:00.500, 00:00:04.500] spans the 4 whole seconds 01..04 (one per bucket); the rows
-- at 00:00:00 (below begin_x) and 00:00:05 (above end_x) are excluded.
SELECT 'countSparkbar with DateTime64 coarser column (col=0, params=3):';
SELECT countSparkbar(4, toDateTime64('2024-01-01 00:00:00.500', 3), toDateTime64('2024-01-01 00:00:04.500', 3))(
    toDateTime64('2024-01-01 00:00:00', 0) + INTERVAL (number) SECOND
) FROM numbers(6);

-- DateTime64 sub-tick range with a single representable column tick (col=0, params=3):
-- the range [00:00:00.500, 00:00:01.500] contains exactly one whole second (00:00:01).
-- A previous implementation rounded both bounds to the same column tick and threw
-- BAD_ARGUMENTS; bucketing at the finest scale aggregates the single matching tick instead.
SELECT 'countSparkbar with DateTime64 single representable tick (col=0, params=3):';
SELECT countSparkbar(2, toDateTime64('2024-01-01 00:00:00.500', 3), toDateTime64('2024-01-01 00:00:01.500', 3))(
    toDateTime64('2024-01-01 00:00:00', 0) + INTERVAL (number) SECOND
) FROM numbers(3);

-- DateTime64 sub-tick range with no representable column tick (col=0, params=3): the range
-- [00:00:00.300, 00:00:00.700] lies entirely between two whole seconds, so no column tick can
-- match and the result is an empty bar (not an exception).
SELECT 'countSparkbar with DateTime64 sub-tick empty range (col=0, params=3):';
SELECT countSparkbar(2, toDateTime64('2024-01-01 00:00:00.300', 3), toDateTime64('2024-01-01 00:00:00.700', 3))(
    toDateTime64('2024-01-01 00:00:00', 0) + INTERVAL (number) SECOND
) FROM numbers(3);

-- Parametric nested aggregate: leading params are forwarded to the nested function,
-- the last 3 are consumed by the combinator (width, begin_x, end_x).
SELECT 'quantileSparkbar with range:';
SELECT quantileSparkbar(0.9, 5, 0, 4)(number % 5, number) FROM numbers(20);

-- Nullable-returning nested function: `avgOrNull` returns `Nullable(Float64)`. The combinator
-- strips the `Nullable` wrapper before its numeric-type check, so the composition is accepted;
-- reverting that `removeNullable` step would reject it with ILLEGAL_TYPE_OF_ARGUMENT. The result
-- of any `-Sparkbar` function is always a String.
SELECT 'avgOrNullSparkbar result type (String):';
SELECT toTypeName(avgOrNullSparkbar(3, 0, 2)(number, number)) FROM numbers(3);

-- A bucket whose nested result is NULL renders as a blank. Here the middle bucket (key = 1)
-- receives only NULL values, so `avgOrNull` returns NULL for it and it is drawn as a space
-- between the two rendered bars. This exercises the `isNullAt` path in `insertResultInto`.
SELECT 'avgOrNullSparkbar with a NULL-only bucket (blank):';
SELECT avgOrNullSparkbar(3, 0, 2)(intDiv(number, 2), if(intDiv(number, 2) = 1, NULL, toInt64(number) * 10)) FROM numbers(6);

-- The `Null` combinator must preserve the nested aggregate's all-null argument contract after
-- `Sparkbar` prepends the x-axis argument. `count(NULL)` produces blank buckets but the result
-- remains a String, rather than being replaced by a `Nothing` aggregate result.
SELECT 'countSparkbar with an all-NULL forwarded argument:';
SELECT toTypeName(countSparkbar(3, 0, 2)(number, CAST(NULL, 'Nullable(UInt8)'))) FROM numbers(3);

-- A `Nullable` *input argument* must not make the result `Nullable(String)`. Without an own null
-- adapter the generic `Null` combinator would reuse the nested function's
-- `returns_default_when_only_null` property (`avg`'s is false) and wrap the whole sparkbar into
-- `Nullable(String)`, so an all-`NULL` input would return `NULL` instead of the empty sparkbar.
SELECT 'avgSparkbar with a Nullable x-axis argument (String, NULLs skipped):';
SELECT toTypeName(avgSparkbar(3, 0, 2)(CAST(number AS Nullable(UInt8)), number)) FROM numbers(3);
SELECT avgSparkbar(3, 0, 2)(if(number = 1, NULL, toUInt8(number)), toInt64(number) * 10) FROM numbers(3);

SELECT 'avgSparkbar with an all-NULL Nullable x-axis (empty sparkbar, not NULL):';
SELECT toTypeName(avgSparkbar(3, 0, 2)(CAST(NULL, 'Nullable(UInt8)'), number)) FROM numbers(3);
SELECT '[' || avgSparkbar(3, 0, 2)(CAST(NULL, 'Nullable(UInt8)'), number) || ']' FROM numbers(3);

SELECT 'sumSparkbar with a Nullable forwarded argument (String, NULLs skipped):';
SELECT toTypeName(sumSparkbar(3, 0, 2)(number, CAST(number AS Nullable(UInt8)))) FROM numbers(3);
SELECT sumSparkbar(3, 0, 2)(number, if(number = 1, NULL, toUInt8(number) * 10)) FROM numbers(3);

SELECT 'countSparkbar with a Nullable x-axis only (unary null adapter):';
SELECT toTypeName(countSparkbar(3, 0, 2)(CAST(number AS Nullable(UInt8)))) FROM numbers(3);
SELECT countSparkbar(3, 0, 2)(if(number = 1, NULL, toUInt8(number))) FROM numbers(3);

-- Error: too few parameters
SELECT countSparkbar(5, 0)(number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Error: extra parameters forwarded to a nested function that takes no parameters
SELECT countSparkbar(5, 0, 9, 1)(number) FROM numbers(10); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }

-- Error: width out of range
SELECT countSparkbar(1, 0, 9)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT countSparkbar(1025, 0, 9)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- Error: begin_x >= end_x
SELECT countSparkbar(5, 5, 5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- Error: unsupported x-axis type (Float64 is not allowed)
SELECT countSparkbar(5, 0, 4)(toFloat64(number)) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: DateTime64 rescaling overflow. A `DateTime64(0)` value of 10^18 rescaled to
-- a `DateTime64(9)` column would require multiplying by 10^9, which exceeds `Int64::max`.
SELECT countSparkbar(5, toDateTime64(1000000000000000000, 0), toDateTime64(2000000000000000000, 0))(
    toDateTime64('2024-01-01 00:00:00', 9)
) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }

-- Error: signed x-axis bounds outside the range of the key type must be rejected instead of
-- wrapping modulo `2^N`. `9223372036854775808` does not fit into `Int64`, and `2147483648`
-- does not fit into the `Int32` domain of a `Date32` key.
SELECT countSparkbar(2, 9223372036854775808, 9223372036854775809)(toInt64(-9223372036854775808)); -- { serverError BAD_ARGUMENTS }
SELECT countSparkbar(2, 0, 9223372036854775808)(toInt64(number)) FROM numbers(4); -- { serverError BAD_ARGUMENTS }
SELECT countSparkbar(2, -2147483649, 0)(toDate32('2000-01-01')); -- { serverError BAD_ARGUMENTS }
SELECT countSparkbar(2, 0, 2147483648)(toDate32('2000-01-01')); -- { serverError BAD_ARGUMENTS }
