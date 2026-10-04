-- The FILTER (WHERE ...) clause on window aggregates: it selects the rows that feed the aggregate but does not change the frame.

DROP TABLE IF EXISTS t_f;
CREATE TABLE t_f (p UInt8, o UInt8, v Int32, flag Nullable(UInt8)) ENGINE = MergeTree ORDER BY (p, o);
INSERT INTO t_f VALUES (1, 1, 10, 1), (1, 2, 20, 0), (1, 3, 30, NULL), (1, 4, 40, 1), (2, 1, 5, 1), (2, 2, 6, NULL), (2, 3, 7, 1);

SELECT '-- Whole partition';
SELECT p, o, v, flag,
    count() FILTER (WHERE flag = 1) OVER (PARTITION BY p) AS flagged,
    sum(v) FILTER (WHERE v > 10) OVER (PARTITION BY p) AS big_sum,
    arraySort(groupArray(v) FILTER (WHERE flag = 1) OVER (PARTITION BY p)) AS flagged_values
FROM t_f ORDER BY p, o;

SELECT '-- Growing frame: the filter chooses the rows, the frame stays the same';
SELECT p, o, v, flag,
    count() OVER w AS frame_rows,
    count() FILTER (WHERE flag = 1) OVER w AS flagged_rows,
    sum(v) FILTER (WHERE v % 20 = 0) OVER w AS sum_of_20s,
    countIf(flag = 1) OVER w AS with_if
FROM t_f WINDOW w AS (PARTITION BY p ORDER BY o) ORDER BY p, o;

SELECT '-- Sliding ROWS frame';
SELECT p, o, v,
    arraySort(groupArray(v) OVER w) AS frame,
    arraySort(groupArray(v) FILTER (WHERE v % 20 != 0) OVER w) AS filtered,
    count(*) FILTER (WHERE v > 5) OVER w AS cnt
FROM t_f WINDOW w AS (PARTITION BY p ORDER BY o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY p, o;

SELECT '-- RANGE frame with peers';
SELECT k, v, count() OVER w AS frame_rows, count() FILTER (WHERE v % 2 = 0) OVER w AS even_rows
FROM (SELECT intDiv(number, 2) AS k, number AS v FROM numbers(6)) WINDOW w AS (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY k, v;

SELECT '-- A NULL condition excludes the row';
SELECT p, o, flag, count() FILTER (WHERE flag) OVER (PARTITION BY p) AS truthy, count() FILTER (WHERE flag IS NULL) OVER (PARTITION BY p) AS nulls FROM t_f ORDER BY p, o;

SELECT '-- FILTER with DISTINCT, with a parametric aggregate and with a named window';
SELECT p, o, v,
    count(DISTINCT v % 3) FILTER (WHERE v > 5) OVER w AS distinct_remainders,
    quantileExact(0.5)(v) FILTER (WHERE flag = 1) OVER w AS median_of_flagged,
    uniqExact(v) FILTER (WHERE v < 30) OVER w AS small_values
FROM t_f WINDOW w AS (PARTITION BY p ORDER BY o) ORDER BY p, o;

SELECT '-- FILTER agrees with the -If combinator';
SELECT countIf(a != b), countIf(c != d), count()
FROM
(
    SELECT
        sum(number) FILTER (WHERE number % 3 = 0) OVER w AS a,
        sumIf(number, number % 3 = 0) OVER w AS b,
        avg(number) FILTER (WHERE number % 2 = 1) OVER w AS c,
        avgIf(number, number % 2 = 1) OVER w AS d
    FROM numbers(3000)
    WINDOW w AS (PARTITION BY number % 7 ORDER BY number ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING)
    SETTINGS max_block_size = 64
);

SELECT '-- FILTER is only for aggregates';
SELECT row_number() FILTER (WHERE number > 0) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT rank() FILTER (WHERE number > 0) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lag(number) FILTER (WHERE number > 0) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
