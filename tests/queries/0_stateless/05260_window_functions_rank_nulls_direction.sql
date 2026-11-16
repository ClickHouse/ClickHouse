-- Ranking functions over a Nullable ORDER BY key with ties: NULLS FIRST and NULLS LAST in both directions.

DROP TABLE IF EXISTS t_rk;
CREATE TABLE t_rk (k Nullable(Int32), id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_rk VALUES (3, 1), (NULL, 2), (1, 3), (3, 4), (NULL, 5), (2, 6), (1, 7);

SELECT '-- ASC NULLS FIRST';
SELECT k, id, rank() OVER w AS r, dense_rank() OVER w AS dr, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd
FROM t_rk WINDOW w AS (ORDER BY k ASC NULLS FIRST) ORDER BY k ASC NULLS FIRST, id;

SELECT '-- ASC NULLS LAST';
SELECT k, id, rank() OVER w AS r, dense_rank() OVER w AS dr, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd
FROM t_rk WINDOW w AS (ORDER BY k ASC NULLS LAST) ORDER BY k ASC NULLS LAST, id;

SELECT '-- DESC NULLS FIRST';
SELECT k, id, rank() OVER w AS r, dense_rank() OVER w AS dr, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd
FROM t_rk WINDOW w AS (ORDER BY k DESC NULLS FIRST) ORDER BY k DESC NULLS FIRST, id;

SELECT '-- DESC NULLS LAST';
SELECT k, id, rank() OVER w AS r, dense_rank() OVER w AS dr, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd
FROM t_rk WINDOW w AS (ORDER BY k DESC NULLS LAST) ORDER BY k DESC NULLS LAST, id;

SELECT '-- row_number with a unique secondary key';
SELECT k, id,
    row_number() OVER (ORDER BY k ASC NULLS FIRST, id) AS rn_nulls_first,
    row_number() OVER (ORDER BY k ASC NULLS LAST, id) AS rn_nulls_last,
    row_number() OVER (ORDER BY k DESC NULLS FIRST, id) AS rn_desc_nulls_first
FROM t_rk ORDER BY k ASC NULLS FIRST, id;

SELECT '-- dense_rank in both directions over a Nullable(String) key, partitioned, many small blocks';
SELECT DISTINCT p, k, dr_asc, dr_desc, cnt
FROM
(
    SELECT p, k,
        dense_rank() OVER (PARTITION BY p ORDER BY k ASC NULLS FIRST) AS dr_asc,
        dense_rank() OVER (PARTITION BY p ORDER BY k DESC NULLS LAST) AS dr_desc,
        count() OVER (PARTITION BY p) AS cnt
    FROM (SELECT number % 3 AS p, if(number % 5 = 0, NULL, toString(number % 7)) AS k FROM numbers(30))
    SETTINGS max_block_size = 4
)
ORDER BY p, k ASC NULLS FIRST;

SELECT '-- dense_rank ASC plus dense_rank DESC is one more than the number of distinct keys in the partition';
SELECT countIf(dr_asc + dr_desc != nd + 1), count()
FROM
(
    SELECT
        dense_rank() OVER (PARTITION BY p ORDER BY k ASC NULLS FIRST) AS dr_asc,
        dense_rank() OVER (PARTITION BY p ORDER BY k DESC NULLS LAST) AS dr_desc,
        uniqExact(k) OVER (PARTITION BY p) + max(isNull(k)) OVER (PARTITION BY p) AS nd
    FROM (SELECT number % 7 AS p, if(number % 11 = 0, NULL, toString(number % 13)) AS k FROM numbers(2000))
    SETTINGS max_block_size = 64
);
