-- RANGE frames with offsets over a Nullable ORDER BY key: the NULL keys form one peer group at the start or at the end.

DROP TABLE IF EXISTS t_nk;
CREATE TABLE t_nk (k Nullable(Int32), v UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_nk VALUES (NULL, 1), (1, 2), (2, 3), (2, 4), (5, 5), (NULL, 6), (8, 7), (9, 8), (NULL, 9);

SELECT '-- ASC NULLS FIRST';
SELECT k, v,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS f_1_1,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)) AS f_unb_1,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)) AS f_1_unb,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)) AS f_cur_2,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS f_3_1
FROM t_nk ORDER BY k ASC NULLS FIRST, v;

SELECT '-- DESC NULLS LAST';
SELECT k, v,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS f_1_1,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)) AS f_unb_1,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)) AS f_1_unb,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)) AS f_cur_2,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS LAST RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS f_3_1
FROM t_nk ORDER BY k DESC NULLS LAST, v;

SELECT '-- Nullable(Date) key with NULLs, 1 PRECEDING AND 1 FOLLOWING';
SELECT d, v, arraySort(groupArray(v) OVER (ORDER BY d ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT if(number % 4 = 0, NULL, toDate('2024-01-01') + intDiv(number, 2)) AS d, number AS v FROM numbers(8)) ORDER BY d ASC NULLS FIRST, v;

SELECT '-- A run of a hundred NULL keys placed first, many small blocks';
SELECT isNull(k) AS null_key, count(), min(c1), max(c1), min(c2), max(c2)
FROM
(
    SELECT k,
        count() OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS c1,
        count() OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS c2
    FROM (SELECT if(number < 100, NULL, toInt32(number)) AS k FROM numbers(200))
    SETTINGS max_block_size = 16
)
GROUP BY null_key ORDER BY null_key;
