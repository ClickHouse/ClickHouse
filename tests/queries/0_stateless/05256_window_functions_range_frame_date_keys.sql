-- RANGE frames whose ORDER BY key is a Date or a DateTime: the offsets are whole days or seconds.

DROP TABLE IF EXISTS t_dates;
CREATE TABLE t_dates (grp UInt8, d Date, v UInt32) ENGINE = MergeTree ORDER BY (grp, d, v);
-- Gaps and repeated days show which rows fall into each frame.
INSERT INTO t_dates VALUES
    (1, '2024-01-01', 1), (1, '2024-01-02', 2), (1, '2024-01-02', 3), (1, '2024-01-05', 4), (1, '2024-01-06', 5), (1, '2024-01-10', 6),
    (2, '2024-02-28', 7), (2, '2024-02-29', 8), (2, '2024-03-01', 9), (2, '2024-03-03', 10);

SELECT '-- Date key, 1 PRECEDING AND 1 FOLLOWING';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date key DESC, 1 PRECEDING AND 1 FOLLOWING';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date key, 3 PRECEDING AND 1 PRECEDING';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date key, 1 FOLLOWING AND 4 FOLLOWING';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d RANGE BETWEEN 1 FOLLOWING AND 4 FOLLOWING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date key, UNBOUNDED PRECEDING AND 2 FOLLOWING';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date key, 2 PRECEDING AND UNBOUNDED FOLLOWING';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date key, CURRENT ROW AND 0 FOLLOWING is the peer group';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d RANGE BETWEEN CURRENT ROW AND 0 FOLLOWING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date key, a frame wider than the whole partition';
SELECT grp, d, v, arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY d RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING)) AS frame
FROM t_dates ORDER BY grp, d, v;

SELECT '-- Date32 and Nullable(Date) keys give the same frames as Date';
SELECT grp, d, v,
    arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY toDate32(d) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame_date32,
    arraySort(groupArray(v) OVER (PARTITION BY grp ORDER BY toNullable(d) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame_nullable
FROM t_dates ORDER BY grp, d, v;

DROP TABLE IF EXISTS t_times;
CREATE TABLE t_times (ts DateTime('UTC'), v UInt32) ENGINE = MergeTree ORDER BY (ts, v);
INSERT INTO t_times VALUES ('2024-01-01 00:00:00', 1), ('2024-01-01 00:00:10', 2), ('2024-01-01 00:00:10', 3), ('2024-01-01 00:00:25', 4), ('2024-01-01 00:01:00', 5), ('2024-01-01 00:01:05', 6);

SELECT '-- DateTime key, 10 PRECEDING AND CURRENT ROW';
SELECT ts, v, arraySort(groupArray(v) OVER (ORDER BY ts RANGE BETWEEN 10 PRECEDING AND CURRENT ROW)) AS frame FROM t_times ORDER BY ts, v;

SELECT '-- DateTime key, 15 PRECEDING AND 15 FOLLOWING';
SELECT ts, v, arraySort(groupArray(v) OVER (ORDER BY ts RANGE BETWEEN 15 PRECEDING AND 15 FOLLOWING)) AS frame FROM t_times ORDER BY ts, v;

SELECT '-- DateTime key DESC, 30 PRECEDING AND 5 FOLLOWING';
SELECT ts, v, arraySort(groupArray(v) OVER (ORDER BY ts DESC RANGE BETWEEN 30 PRECEDING AND 5 FOLLOWING)) AS frame FROM t_times ORDER BY ts, v;

SELECT '-- DateTime key, 60 FOLLOWING AND 60 FOLLOWING: only a row exactly one minute later';
SELECT ts, v, arraySort(groupArray(v) OVER (ORDER BY ts RANGE BETWEEN 60 FOLLOWING AND 60 FOLLOWING)) AS frame FROM t_times ORDER BY ts, v;

SELECT '-- A wide DateTime frame with a median over many blocks matches a brute-force computation';
WITH src AS (SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + intDiv(number * 7, 3) AS ts, toUInt32((number * 7919) % 1000) AS v FROM numbers(3000))
SELECT count(), countIf(w != b)
FROM
(
    SELECT
        ts,
        medianExact(v) OVER (ORDER BY ts RANGE BETWEEN 100 PRECEDING AND CURRENT ROW) AS w,
        arrayReduce('medianExact', arrayMap(x -> x.2, arrayFilter(x -> (x.1 <= ts) AND (x.1 >= (ts - 100)), (SELECT groupArray((ts, v)) FROM src)))) AS b
    FROM src
    SETTINGS max_block_size = 128
);
