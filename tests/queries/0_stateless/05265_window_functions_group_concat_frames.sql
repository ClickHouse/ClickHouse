-- groupConcat as a window function: growing, sliding and whole-partition frames, separators, limits and NULLs.

DROP TABLE IF EXISTS t_gc;
CREATE TABLE t_gc (p UInt8, o UInt8, s Nullable(String)) ENGINE = MergeTree ORDER BY (p, o);
INSERT INTO t_gc VALUES (1, 1, 'a'), (1, 2, 'b'), (1, 3, NULL), (1, 4, 'd'), (2, 1, 'x'), (2, 2, NULL), (2, 3, 'z');

SELECT '-- Growing frame, default separator';
SELECT p, o, s, groupConcat(s) OVER (PARTITION BY p ORDER BY o) AS c FROM t_gc ORDER BY p, o;

SELECT '-- Growing frame with a separator';
SELECT p, o, s, groupConcat(', ')(s) OVER (PARTITION BY p ORDER BY o) AS c FROM t_gc ORDER BY p, o;

SELECT '-- Separator and a limit of two values';
SELECT p, o, s, groupConcat(',', 2)(s) OVER (PARTITION BY p ORDER BY o) AS c FROM t_gc ORDER BY p, o;

SELECT '-- Whole partition';
SELECT p, o, s, groupConcat('|')(s) OVER (PARTITION BY p ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS c FROM t_gc ORDER BY p, o;

SELECT '-- Sliding frame 1 PRECEDING AND 1 FOLLOWING';
SELECT p, o, s, groupConcat('|')(s) OVER (PARTITION BY p ORDER BY o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS c FROM t_gc ORDER BY p, o;

SELECT '-- Frame ahead of the current row: empty at the end of the partition';
SELECT p, o, s, groupConcat('-')(s) OVER (PARTITION BY p ORDER BY o ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS c FROM t_gc ORDER BY p, o;

SELECT '-- Non-string values are converted to strings';
SELECT number,
    groupConcat(',')(number) OVER w AS n,
    groupConcat(',')(toDate('2024-01-01') + number) OVER w AS d,
    groupConcat(',')([number, number]) OVER w AS arr,
    groupConcat(',')(toLowCardinality(toString(number))) OVER w AS lc
FROM numbers(3) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT '-- Distinct values';
SELECT number, groupConcatDistinct(',')(toString(number % 2)) OVER (ORDER BY number) AS c FROM numbers(4) ORDER BY number;

SELECT '-- The separator must be a constant';
SELECT groupConcat(toString(number))(toString(number)) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }

SELECT '-- RANGE frame over a unique key and many small blocks';
SELECT k, groupConcat(',')(toString(k)) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS c
FROM (SELECT number * 2 AS k FROM numbers(8)) ORDER BY k SETTINGS max_block_size = 3;
