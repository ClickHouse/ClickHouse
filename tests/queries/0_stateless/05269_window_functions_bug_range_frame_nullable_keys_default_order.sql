-- RANGE frames with offsets over a Nullable ORDER BY key when the NULL keys sort as the largest values:
-- ASC NULLS LAST, which is the default order, and DESC NULLS FIRST.
-- The frames below are wrong. This test pins the wrong results so that a fix has to update the reference.
-- The correct rule: a row with a NULL key gets only the NULL rows as its frame, and a row with a value gets
-- only the rows whose key is not NULL and lies within the offsets. With ASC NULLS FIRST and DESC NULLS LAST
-- the frames are already correct, see 05259_window_functions_range_frame_nullable_keys.

SELECT '-- Default order: the frame of 5 wrongly includes the NULL row and the frame of the NULL row wrongly includes 5';
-- The correct counts are 2, 2, 1, 1.
SELECT k, count() OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS c
FROM values('k Nullable(Int32)', 1, 2, 5, NULL) ORDER BY k NULLS LAST;

SELECT '-- DESC NULLS FIRST: every frame wrongly covers the whole partition';
-- The correct counts are 2, 2, 1, 1.
SELECT k, count() OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS c
FROM values('k Nullable(Int32)', 1, 2, 5, NULL) ORDER BY k NULLS LAST;

DROP TABLE IF EXISTS t_nk;
CREATE TABLE t_nk (k Nullable(Int32), v UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_nk VALUES (NULL, 1), (1, 2), (2, 3), (2, 4), (5, 5), (NULL, 6), (8, 7), (9, 8), (NULL, 9);

SELECT '-- ASC NULLS LAST: the NULL rows 1, 6, 9 leak into the frames of the keys 8 and 9, and the rows 7, 8 leak into the frames of the NULL rows';
-- The correct frames of the value rows are those of the ASC NULLS FIRST case in 05259 with the NULL rows
-- moved from the UNBOUNDED PRECEDING side to the UNBOUNDED FOLLOWING side; a NULL row gets [1,6,9] in every
-- column except f_unb_1, which is the whole partition.
SELECT k, v,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS f_1_1,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)) AS f_unb_1,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)) AS f_1_unb,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)) AS f_cur_2,
    arraySort(groupArray(v) OVER (ORDER BY k ASC NULLS LAST RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS f_3_1
FROM t_nk ORDER BY k ASC NULLS LAST, v;

SELECT '-- DESC NULLS FIRST: frames that should stop at the offset run to the partition end';
-- The correct frames are those of the DESC NULLS LAST case in 05259 with the NULL rows moved from the
-- UNBOUNDED FOLLOWING side to the UNBOUNDED PRECEDING side.
SELECT k, v,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS f_1_1,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)) AS f_unb_1,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)) AS f_1_unb,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)) AS f_cur_2,
    arraySort(groupArray(v) OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS f_3_1
FROM t_nk ORDER BY k DESC NULLS FIRST, v;

SELECT '-- Nullable(Date) key, NULLS LAST: the rows 0 and 4 with a NULL key wrongly join the frames of the last two days';
-- The correct frames are [1,2,3], [1,2,3,5], [1,2,3,5], [2,3,5,6,7], [5,6,7], [5,6,7], [0,4], [0,4].
SELECT d, v, arraySort(groupArray(v) OVER (ORDER BY d ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT if(number % 4 = 0, NULL, toDate('2024-01-01') + intDiv(number, 2)) AS d, number AS v FROM numbers(8)) ORDER BY d ASC NULLS LAST, v;

SELECT '-- A run of a hundred NULL keys at the end: the last value row wrongly counts the NULL run, so max(c) is 200 instead of 100';
SELECT isNull(k) AS null_key, count(), min(c), max(c)
FROM
(
    SELECT k, count() OVER (ORDER BY k ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS c
    FROM (SELECT if(number < 100, NULL, toInt32(number)) AS k FROM numbers(200))
    SETTINGS max_block_size = 16
)
GROUP BY null_key ORDER BY null_key;
