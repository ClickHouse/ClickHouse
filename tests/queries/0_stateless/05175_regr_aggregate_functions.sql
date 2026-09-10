-- The SQL standard regression aggregates. Every one of them takes the dependent variable first.

SELECT 'a line through the origin';
SELECT
    regr_count(y, x),
    regr_avgx(y, x),
    regr_avgy(y, x),
    regr_sxx(y, x),
    regr_syy(y, x),
    regr_sxy(y, x),
    regr_slope(y, x),
    regr_intercept(y, x),
    regr_r2(y, x)
FROM VALUES('x Float64, y Float64', (1, 2), (2, 4), (3, 6), (4, 8));

SELECT 'a line that does not pass through the origin';
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x)
FROM VALUES('x Float64, y Float64', (1, 5), (2, 7), (3, 9));

SELECT 'the slope agrees with simpleLinearRegression';
SELECT regr_slope(y, x) = simpleLinearRegression(x, y).1, regr_intercept(y, x) = simpleLinearRegression(x, y).2
FROM VALUES('x Float64, y Float64', (1, 5), (2, 7), (3, 10), (7, 21));

SELECT 'no rows';
SELECT regr_count(y, x), regr_slope(y, x), regr_avgx(y, x), regr_r2(y, x)
FROM VALUES('x Float64, y Float64', (1, 1)) WHERE x > 100;

SELECT 'a single row leaves the line undetermined';
SELECT regr_count(y, x), regr_sxx(y, x), regr_sxy(y, x), regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x)
FROM VALUES('x Float64, y Float64', (1, 2));

SELECT 'a constant x is a vertical line';
SELECT regr_sxx(y, x), regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x)
FROM VALUES('x Float64, y Float64', (3, 1), (3, 2), (3, 5));

SELECT 'a constant y is explained completely';
SELECT regr_syy(y, x), regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x)
FROM VALUES('x Float64, y Float64', (1, 4), (2, 4), (3, 4));

SELECT 'rows where either variable is NULL do not take part';
SELECT regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x), regr_slope(y, x)
FROM VALUES('x Nullable(Float64), y Nullable(Float64)', (1, 2), (2, 4), (NULL, 6), (4, NULL), (3, 6));

SELECT 'integer arguments';
SELECT regr_count(y, x), regr_slope(y, x), regr_intercept(y, x)
FROM VALUES('x UInt8, y Int32', (1, 2), (2, 4), (3, 6));

SELECT 'the names are case insensitive';
SELECT REGR_SLOPE(y, x), Regr_Intercept(y, x)
FROM VALUES('x Float64, y Float64', (1, 2), (2, 4));

SELECT 'the arguments are ordered as regr_f(y, x)';
SELECT regr_slope(y, x), regr_slope(x, y)
FROM VALUES('x Float64, y Float64', (1, 2), (2, 4), (3, 6));

SELECT 'they work as window functions';
SELECT x, regr_slope(y, x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM VALUES('x Float64, y Float64', (1, 2), (2, 4), (3, 6))
ORDER BY x;

SELECT 'partial states merge into the whole fit';
SELECT regr_slopeMerge(a), regr_interceptMerge(b) FROM (
    SELECT regr_slopeState(y, x) AS a, regr_interceptState(y, x) AS b FROM VALUES('x Float64, y Float64', (1, 2), (2, 4))
    UNION ALL
    SELECT regr_slopeState(y, x), regr_interceptState(y, x) FROM VALUES('x Float64, y Float64', (3, 6), (4, 8))
);

SELECT 'wrong argument types are rejected';
SELECT regr_slope('a', 'b'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT regr_slope(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
