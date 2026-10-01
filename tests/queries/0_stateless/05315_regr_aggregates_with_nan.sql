-- A NaN or an infinity in the data leaves the fit undefined, and every `regr_*` result says so.
-- The sums of squares are clamped at zero, and a zero sum of squares is read as a constant column,
-- so a NaN that the clamp turned into a zero answered `regr_r2` with a perfect fit of 1.

SELECT 'a NaN in y';
SELECT regr_r2(y, x), regr_syy(y, x), regr_sxx(y, x), regr_slope(y, x), regr_intercept(y, x), regr_avgy(y, x)
FROM VALUES('x Float64, y Float64', (1, nan), (2, 2), (3, 3));

SELECT 'a NaN in x';
SELECT regr_r2(y, x), regr_sxx(y, x), regr_syy(y, x), regr_slope(y, x)
FROM VALUES('x Float64, y Float64', (nan, 1), (2, 2), (3, 3));

SELECT 'an infinity in y';
SELECT regr_r2(y, x), regr_syy(y, x), regr_slope(y, x)
FROM VALUES('x Float64, y Float64', (1, inf), (2, 2), (3, 3));

SELECT 'the count is the number of rows, whatever they hold';
SELECT regr_count(y, x) FROM VALUES('x Float64, y Float64', (1, nan), (2, 2), (3, 3));

SELECT 'a constant column is still a constant column, not a NaN';
SELECT regr_r2(y, x), regr_syy(y, x), regr_sxx(y, x), regr_slope(y, x)
FROM VALUES('x Float64, y Float64', (1, 5), (2, 5), (3, 5));
SELECT regr_r2(y, x), regr_sxx(y, x), regr_slope(y, x)
FROM VALUES('x Float64, y Float64', (5, 1), (5, 2), (5, 3));

SELECT 'a fit without either is unchanged';
SELECT regr_r2(y, x), regr_slope(y, x), regr_intercept(y, x)
FROM VALUES('x Float64, y Float64', (1, 2), (2, 4), (3, 6));
