-- Argument checks and edge cases of nth_value and ntile.

SELECT '-- ntile rejects NULL, Nullable, fractional and string bucket counts';
SELECT ntile(NULL) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT ntile(toNullable(2)) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT ntile(2.5) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT ntile('2') OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }

SELECT '-- nth_value takes exactly two arguments';
SELECT nth_value(number) OVER (ORDER BY number) FROM numbers(3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT nth_value(number, 1, 2) OVER (ORDER BY number) FROM numbers(3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- The offset of nth_value must be an integer';
SELECT nth_value(number, NULL) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT nth_value(number, toNullable(1)) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT nth_value(number, 1.5) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT nth_value(number, '2') OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }

SELECT '-- The offset may come from a column and differ per row';
SELECT number, n,
    nth_value(number * 10, n) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS whole,
    nth_value(number * 10, n) OVER (ORDER BY number) AS running
FROM (SELECT number, 5 - number AS n FROM numbers(5)) ORDER BY number;

SELECT '-- A per-row offset of zero is an error';
SELECT nth_value(number, number) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }

SELECT '-- NULL values inside the frame are returned as is; an offset past the frame gives NULL for Nullable input';
SELECT number, x, nth_value(x, 2) OVER w AS second, nth_value(x, 4) OVER w AS fourth, nth_value(x, 6) OVER w AS sixth
FROM (SELECT number, if(number % 3 = 1, NULL, toInt32(number)) AS x FROM numbers(5))
WINDOW w AS (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY number;

SELECT '-- An offset past the frame gives the default value of the type for non-Nullable input';
SELECT number, nth_value(number, 10) OVER w AS n, nth_value(toString(number), 10) OVER w AS s, nth_value([number], 10) OVER w AS a, nth_value(toDate('2024-01-01') + number, 10) OVER w AS d
FROM numbers(3) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT '-- Non-numeric argument types';
SELECT number,
    nth_value(toString(number), 2) OVER w AS s,
    nth_value([number, number], 2) OVER w AS arr,
    nth_value((number, toString(number)), 2) OVER w AS tup,
    nth_value(toLowCardinality(toString(number)), 2) OVER w AS lc,
    nth_value(toDate('2024-01-01') + number, 2) OVER w AS d,
    nth_value(toDecimal64(number, 2), 2) OVER w AS dec,
    nth_value(map(number, toString(number)), 2) OVER w AS m
FROM numbers(3) WINDOW w AS (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY number;

SELECT '-- Single-row frames: only the first value exists';
SELECT number,
    nth_value(number, 1) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS rows_first,
    nth_value(number, 2) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS rows_second,
    nth_value(number, 1) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS range_first,
    nth_value(number, 2) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS range_second
FROM numbers(3) ORDER BY number;

SELECT '-- Running frame without PARTITION BY across many small blocks';
SELECT countIf(n2 != if(number >= 1, 1, 0)), countIf(n3 != if(number >= 2, 2, 0)), count()
FROM
(
    SELECT number,
        nth_value(number, 2) OVER (ORDER BY number ROWS UNBOUNDED PRECEDING) AS n2,
        nth_value(number, 3) OVER (ORDER BY number ROWS UNBOUNDED PRECEDING) AS n3
    FROM numbers(1000)
    SETTINGS max_block_size = 7
);
