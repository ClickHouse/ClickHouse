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

-- Parametric nested aggregate: leading params are forwarded to the nested function,
-- the last 3 are consumed by the combinator (width, begin_x, end_x).
SELECT 'quantileSparkbar with range:';
SELECT quantileSparkbar(0.9, 5, 0, 4)(number % 5, number) FROM numbers(20);

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
