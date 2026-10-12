-- RANGE frames with offsets on integer, float, boolean and enum keys, with repeated keys (peers) and gaps.

DROP TABLE IF EXISTS t_range;
CREATE TABLE t_range (k Int32, v UInt32) ENGINE = MergeTree ORDER BY (k, v);
INSERT INTO t_range VALUES (1, 1), (1, 2), (2, 3), (4, 4), (4, 5), (4, 6), (7, 7), (8, 8), (10, 9);

SELECT '-- 0 PRECEDING AND 0 FOLLOWING equals CURRENT ROW AND CURRENT ROW: the peer group';
SELECT k, v,
    arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING)) AS zero_offsets,
    arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND CURRENT ROW)) AS current_row
FROM t_range ORDER BY k, v;

SELECT '-- 3 PRECEDING AND 0 PRECEDING equals 3 PRECEDING AND CURRENT ROW';
SELECT k, v,
    arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 3 PRECEDING AND 0 PRECEDING)) AS zero_preceding,
    arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 3 PRECEDING AND CURRENT ROW)) AS current_row
FROM t_range ORDER BY k, v;

SELECT '-- 0 FOLLOWING AND 3 FOLLOWING equals CURRENT ROW AND 3 FOLLOWING';
SELECT k, v,
    arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 0 FOLLOWING AND 3 FOLLOWING)) AS zero_following,
    arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND 3 FOLLOWING)) AS current_row
FROM t_range ORDER BY k, v;

SELECT '-- 1 FOLLOWING AND 3 FOLLOWING';
SELECT k, v, arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING)) AS frame FROM t_range ORDER BY k, v;

SELECT '-- 3 PRECEDING AND 1 PRECEDING';
SELECT k, v, arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS frame FROM t_range ORDER BY k, v;

SELECT '-- DESC, 1 FOLLOWING AND 3 FOLLOWING';
SELECT k, v, arraySort(groupArray(v) OVER (ORDER BY k DESC RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING)) AS frame FROM t_range ORDER BY k, v;

SELECT '-- DESC, 3 PRECEDING AND 1 PRECEDING';
SELECT k, v, arraySort(groupArray(v) OVER (ORDER BY k DESC RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS frame FROM t_range ORDER BY k, v;

SELECT '-- An offset larger than the key spread covers the whole partition';
SELECT k, v, arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING)) AS frame FROM t_range ORDER BY k, v;

SELECT '-- Signed key crossing zero, 2 PRECEDING AND 2 FOLLOWING';
SELECT k, arraySort(groupArray(k) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING)) AS frame
FROM (SELECT toInt8(number) - 3 AS k FROM numbers(7)) ORDER BY k;

SELECT '-- Signed key DESC, 2 PRECEDING AND 1 FOLLOWING';
SELECT k, arraySort(groupArray(k) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT toInt8(number) - 3 AS k FROM numbers(7)) ORDER BY k;

SELECT '-- Float key with repeated values, 1 PRECEDING AND 1 FOLLOWING';
SELECT k, v, arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT [0.5, 0.5, 1.25, 1.5, 2.5, 3.][number + 1] AS k, number + 1 AS v FROM numbers(6)) ORDER BY k, v;

SELECT '-- Float32 key, 1 PRECEDING AND CURRENT ROW';
SELECT k, v, arraySort(groupArray(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)) AS frame
FROM (SELECT toFloat32(number) / 2 AS k, number AS v FROM numbers(6)) ORDER BY k, v;

SELECT '-- Bool key: the peers of each value and the previous value';
SELECT b, v,
    arraySort(groupArray(v) OVER (ORDER BY b RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING)) AS peers,
    arraySort(groupArray(v) OVER (ORDER BY b RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)) AS with_previous
FROM (SELECT CAST(number % 2, 'Bool') AS b, number AS v FROM numbers(5)) ORDER BY b, v;

SELECT '-- Enum key: offsets count enum values';
SELECT e, v, arraySort(groupArray(v) OVER (ORDER BY e RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)) AS frame
FROM (SELECT CAST(number % 3, 'Enum8(\'a\' = 0, \'b\' = 1, \'c\' = 2)') AS e, number AS v FROM numbers(6)) ORDER BY e, v;

SELECT '-- Value functions in RANGE offset frames with one thread';
SELECT p, k, v, first_value(v) OVER w AS f, last_value(v) OVER w AS l, nth_value(v, 2) OVER w AS n
FROM (SELECT number % 3 AS p, intDiv(number, 3) * 2 AS k, number AS v FROM numbers(18))
WINDOW w AS (PARTITION BY p ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING)
ORDER BY p, k, v
SETTINGS max_threads = 1;

SELECT '-- The same with several threads';
SELECT p, k, v, first_value(v) OVER w AS f, last_value(v) OVER w AS l, nth_value(v, 2) OVER w AS n
FROM (SELECT number % 3 AS p, intDiv(number, 3) * 2 AS k, number AS v FROM numbers(18))
WINDOW w AS (PARTITION BY p ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING)
ORDER BY p, k, v
SETTINGS max_threads = 4;
