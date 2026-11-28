-- Aggregates that are rarely used as window functions: statistics, bit aggregates, topK, regression, argMin/argMax and count over Nullable columns.

DROP TABLE IF EXISTS t_ma;
CREATE TABLE t_ma (p UInt8, i UInt8, x Float64, y Float64, b Bool, n Nullable(UInt32), s String) ENGINE = MergeTree ORDER BY (p, i);
INSERT INTO t_ma VALUES
    (1, 1, 1, 2, true, 10, 'a'), (1, 2, 2, 4.5, true, NULL, 'a'), (1, 3, 4, 8, false, 30, 'b'), (1, 4, 8, 15, true, 30, 'a'),
    (2, 1, 3, 3, false, NULL, 'q'), (2, 2, 3, 3, false, NULL, 'q'), (2, 3, 6, 7, true, 5, 'r');

SELECT '-- Moments and dispersion over the whole partition';
SELECT p, i, round(varPop(x) OVER w, 6) AS var_pop, round(varSamp(x) OVER w, 6) AS var_samp, round(stddevPop(x) OVER w, 6) AS stddev_pop, round(skewPop(x) OVER w, 6) AS skew_pop, round(kurtPop(x) OVER w, 6) AS kurt_pop, round(covarPop(x, y) OVER w, 6) AS covar_pop, round(corr(x, y) OVER w, 6) AS correlation
FROM t_ma WINDOW w AS (PARTITION BY p) ORDER BY p, i;

SELECT '-- The same over a sliding frame of three rows';
SELECT p, i, round(stddevPop(x) OVER w, 6) AS stddev_pop, round(skewPop(x) OVER w, 6) AS skew_pop, round(kurtSamp(x) OVER w, 6) AS kurt_samp, round(corr(x, y) OVER w, 6) AS correlation
FROM t_ma WINDOW w AS (PARTITION BY p ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY p, i;

SELECT '-- Regression over the partition';
SELECT p, i, round(lr.1, 6) AS slope, round(lr.2, 6) AS intercept
FROM (SELECT p, i, simpleLinearRegression(x, y) OVER (PARTITION BY p) AS lr FROM t_ma) ORDER BY p, i;

SELECT '-- Bit aggregates and min/max over Bool act as AND/OR over the frame';
SELECT p, i, b, min(b) OVER w AS all_true, max(b) OVER w AS any_true, groupBitAnd(toUInt8(b)) OVER w AS bit_and, groupBitOr(toUInt8(b)) OVER w AS bit_or, groupBitXor(toUInt8(b)) OVER w AS bit_xor, sum(b) OVER w AS true_count
FROM t_ma WINDOW w AS (PARTITION BY p ORDER BY i) ORDER BY p, i;

SELECT '-- topK, entropy and uniq over a growing frame';
SELECT p, i, s, arraySort(topK(2)(s) OVER w) AS top2, round(entropy(s) OVER w, 6) AS ent, uniq(s) OVER w AS u, uniqExact(s) OVER w AS ue
FROM t_ma WINDOW w AS (PARTITION BY p ORDER BY i) ORDER BY p, i;

SELECT '-- argMin and argMax over the partition and over a sliding frame';
SELECT p, i, x, argMax(i, x) OVER (PARTITION BY p) AS i_of_max_x, argMin(i, x) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS i_of_min_x_nearby, argMax(s, i) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_s
FROM t_ma ORDER BY p, i;

SELECT '-- count over a Nullable column counts non-NULL values only';
SELECT p, i, n, count() OVER w AS rows, count(n) OVER w AS non_null, count(p) OVER w AS non_nullable_column, uniqExact(n) OVER w AS distinct_non_null, sum(n) OVER w AS s, avg(n) OVER w AS a
FROM t_ma WINDOW w AS (PARTITION BY p ORDER BY i) ORDER BY p, i;
