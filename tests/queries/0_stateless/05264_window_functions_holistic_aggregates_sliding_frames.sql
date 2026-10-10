-- Quantile-like aggregates over sliding, shrinking and empty frames.

DROP TABLE IF EXISTS t_q;
CREATE TABLE t_q (i UInt8, v UInt32) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_q SELECT number, [5, 3, 8, 1, 9, 2, 7, 4, 6, 0][number + 1] FROM numbers(10);

SELECT '-- ROWS 1 PRECEDING AND 1 FOLLOWING';
SELECT i, v, medianExact(v) OVER w AS med, quantileExact(0.25)(v) OVER w AS q25, quantilesExact(0.25, 0.75)(v) OVER w AS qs, quantile(0.5)(v) OVER w AS q_interpolated, arraySort(groupArray(v) OVER w) AS frame
FROM t_q WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY i;

SELECT '-- ROWS 2 PRECEDING AND CURRENT ROW';
SELECT i, v, medianExact(v) OVER w AS med, quantileExact(0.25)(v) OVER w AS q25, quantilesExact(0.25, 0.75)(v) OVER w AS qs, quantile(0.5)(v) OVER w AS q_interpolated, arraySort(groupArray(v) OVER w) AS frame
FROM t_q WINDOW w AS (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) ORDER BY i;

SELECT '-- ROWS CURRENT ROW AND 2 FOLLOWING';
SELECT i, v, medianExact(v) OVER w AS med, quantileExact(0.25)(v) OVER w AS q25, quantilesExact(0.25, 0.75)(v) OVER w AS qs, quantile(0.5)(v) OVER w AS q_interpolated, arraySort(groupArray(v) OVER w) AS frame
FROM t_q WINDOW w AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) ORDER BY i;

SELECT '-- ROWS 2 PRECEDING AND 1 PRECEDING: the first row has an empty frame';
SELECT i, v, medianExact(v) OVER w AS med, quantile(0.5)(v) OVER w AS q_interpolated, quantilesExact(0.5)(v) OVER w AS qs, arraySort(groupArray(v) OVER w) AS frame
FROM t_q WINDOW w AS (ORDER BY i ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) ORDER BY i;

SELECT '-- ROWS 1 FOLLOWING AND 2 FOLLOWING: the last row has an empty frame';
SELECT i, v, medianExact(v) OVER w AS med, quantile(0.5)(v) OVER w AS q_interpolated, quantilesExact(0.5)(v) OVER w AS qs, arraySort(groupArray(v) OVER w) AS frame
FROM t_q WINDOW w AS (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) ORDER BY i;

SELECT '-- RANGE frame with peers';
SELECT k, v, medianExact(v) OVER w AS med, quantileExact(0.25)(v) OVER w AS q25, arraySort(groupArray(v) OVER w) AS frame
FROM (SELECT intDiv(i, 3) AS k, v FROM t_q) WINDOW w AS (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY k, v;

SELECT '-- Values of other types';
SELECT i,
    medianExact(toFloat64(v) / 4) OVER w AS f,
    medianExact(toDecimal64(v, 2)) OVER w AS dec,
    medianExact(toDate('2024-01-01') + v) OVER w AS d,
    medianExact(toDateTime('2024-01-01 00:00:00', 'UTC') + v) OVER w AS dt,
    quantile(0.5)(toInt16(v) - 5) OVER w AS signed_interpolated
FROM t_q WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY i;

SELECT '-- Scattered NULLs are skipped, a frame of only NULLs gives NULL';
SELECT i, x, medianExact(x) OVER w AS med, quantile(0.5)(x) OVER w AS q_interpolated, quantilesExact(0.5)(x) OVER w AS qs, arraySort(groupArray(x) OVER w) AS frame
FROM (SELECT i, if(i IN (1, 2, 3, 7), NULL, v) AS x FROM t_q) WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY i;

SELECT '-- Aggregates over an empty frame return their default';
SELECT i, sum(v) OVER w AS s, count(v) OVER w AS c, avg(v) OVER w AS a, min(v) OVER w AS mn, max(v) OVER w AS mx, any(v) OVER w AS an, argMax(toString(v), v) OVER w AS am,
    groupArray(v) OVER w AS ga, uniqExact(v) OVER w AS u, quantileExact(0.5)(v) OVER w AS qe, quantile(0.5)(v) OVER w AS q, quantiles(0.5)(v) OVER w AS qs, quantilesExact(0.5)(v) OVER w AS qse, max(toNullable(v)) OVER w AS mxn
FROM t_q WINDOW w AS (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) ORDER BY i DESC LIMIT 2;

SELECT '-- A wide sliding frame over many blocks matches a brute-force computation';
WITH src AS (SELECT number AS i, toUInt32((number * 7919) % 1009) AS v FROM numbers(2000))
SELECT count(), countIf(w != b)
FROM
(
    SELECT i,
        quantileExact(0.5)(v) OVER (ORDER BY i ROWS BETWEEN 20 PRECEDING AND 20 FOLLOWING) AS w,
        arrayReduce('quantileExact(0.5)', arraySlice((SELECT groupArray(v) FROM (SELECT v FROM src ORDER BY i)), greatest(toInt64(i) - 20, 0) + 1, least(toInt64(i) + 20, 1999) - greatest(toInt64(i) - 20, 0) + 1)) AS b
    FROM src
    SETTINGS max_block_size = 64
);

SELECT '-- A running groupArray over many blocks keeps every row';
SELECT sum(length(a)), max(length(a)), sum(arraySum(a))
FROM (SELECT groupArray(number) OVER (ORDER BY number ROWS UNBOUNDED PRECEDING) AS a FROM numbers(2000) SETTINGS max_block_size = 33);
