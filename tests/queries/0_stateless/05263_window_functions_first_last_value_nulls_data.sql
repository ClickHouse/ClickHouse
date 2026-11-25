-- first_value, last_value, any and anyLast over data that contains NULLs: by default NULLs are skipped, RESPECT NULLS keeps them.

DROP TABLE IF EXISTS t_fv;
CREATE TABLE t_fv (p UInt8, o UInt8, x Nullable(Int32)) ENGINE = MergeTree ORDER BY (p, o);
-- Partition 1 has NULLs at both ends and in the middle, partition 2 has no NULLs, partition 3 is all NULL.
INSERT INTO t_fv VALUES (1, 1, NULL), (1, 2, 10), (1, 3, NULL), (1, 4, 30), (1, 5, NULL), (2, 1, 5), (2, 2, 6), (2, 3, 7), (3, 1, NULL), (3, 2, NULL);

SELECT '-- Default frame';
SELECT p, o, x,
    first_value(x) OVER w AS fv, first_value(x) RESPECT NULLS OVER w AS fv_respect, first_value(x) IGNORE NULLS OVER w AS fv_ignore,
    last_value(x) OVER w AS lv, last_value(x) RESPECT NULLS OVER w AS lv_respect, last_value(x) IGNORE NULLS OVER w AS lv_ignore
FROM t_fv WINDOW w AS (PARTITION BY p ORDER BY o) ORDER BY p, o;

SELECT '-- Whole partition';
SELECT p, o, x,
    first_value(x) OVER w AS fv, first_value(x) RESPECT NULLS OVER w AS fv_respect, first_value(x) IGNORE NULLS OVER w AS fv_ignore,
    last_value(x) OVER w AS lv, last_value(x) RESPECT NULLS OVER w AS lv_respect, last_value(x) IGNORE NULLS OVER w AS lv_ignore
FROM t_fv WINDOW w AS (PARTITION BY p ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY p, o;

SELECT '-- Frame ending before the current row: the first row of each partition has an empty frame';
SELECT p, o, x,
    first_value(x) OVER w AS fv, first_value(x) RESPECT NULLS OVER w AS fv_respect,
    last_value(x) OVER w AS lv, last_value(x) RESPECT NULLS OVER w AS lv_respect
FROM t_fv WINDOW w AS (PARTITION BY p ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) ORDER BY p, o;

SELECT '-- Frame starting after the current row: the last row of each partition has an empty frame';
SELECT p, o, x,
    first_value(x) OVER w AS fv, first_value(x) RESPECT NULLS OVER w AS fv_respect,
    last_value(x) OVER w AS lv, last_value(x) RESPECT NULLS OVER w AS lv_respect
FROM t_fv WINDOW w AS (PARTITION BY p ORDER BY o ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) ORDER BY p, o;

SELECT '-- Sliding frame 1 PRECEDING AND 1 FOLLOWING';
SELECT p, o, x,
    first_value(x) OVER w AS fv, first_value(x) RESPECT NULLS OVER w AS fv_respect,
    last_value(x) OVER w AS lv, last_value(x) RESPECT NULLS OVER w AS lv_respect
FROM t_fv WINDOW w AS (PARTITION BY p ORDER BY o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY p, o;

SELECT '-- any and anyLast behave like first_value and last_value';
SELECT p, o, x, any(x) OVER w AS a, any(x) RESPECT NULLS OVER w AS a_respect, anyLast(x) OVER w AS al, anyLast(x) RESPECT NULLS OVER w AS al_respect
FROM t_fv WINDOW w AS (PARTITION BY p ORDER BY o) ORDER BY p, o;

SELECT '-- Nullable(String) and LowCardinality(Nullable(String)) values';
SELECT p, o, s,
    first_value(s) OVER w AS fv, first_value(s) RESPECT NULLS OVER w AS fv_respect,
    last_value(s) IGNORE NULLS OVER w AS lv_ignore, last_value(s) RESPECT NULLS OVER w AS lv_respect,
    first_value(toLowCardinality(s)) RESPECT NULLS OVER w AS fv_lc_respect, last_value(toLowCardinality(s)) OVER w AS lv_lc
FROM (SELECT p, o, if(x IS NULL, NULL, toString(x)) AS s FROM t_fv)
WINDOW w AS (PARTITION BY p ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY p, o;

SELECT '-- Many small blocks: skipping NULLs in a running frame equals the running maximum of the non-NULL values';
SELECT countIf(lv_ignore != mx), countIf(fv_respect IS NOT NULL), countIf(fv_ignore != 1), count()
FROM
(
    SELECT
        last_value(x) IGNORE NULLS OVER w AS lv_ignore,
        max(x) OVER w AS mx,
        first_value(x) RESPECT NULLS OVER w AS fv_respect,
        first_value(x) IGNORE NULLS OVER w AS fv_ignore
    FROM (SELECT number, if(number % 3 = 0, NULL, toInt32(number)) AS x FROM numbers(10000))
    WINDOW w AS (ORDER BY number ROWS UNBOUNDED PRECEDING)
    SETTINGS max_block_size = 100
);

SELECT '-- RESPECT NULLS is rejected for functions that do not pick a value';
SELECT rank() RESPECT NULLS OVER (ORDER BY number) FROM numbers(3); -- { serverError NOT_IMPLEMENTED }
SELECT row_number() RESPECT NULLS OVER (ORDER BY number) FROM numbers(3); -- { serverError NOT_IMPLEMENTED }
SELECT lag(number) RESPECT NULLS OVER (ORDER BY number) FROM numbers(3); -- { serverError NOT_IMPLEMENTED }
SELECT nth_value(number, 1) RESPECT NULLS OVER (ORDER BY number) FROM numbers(3); -- { serverError NOT_IMPLEMENTED }
