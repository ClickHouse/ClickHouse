-- Interval constants are accepted as RANGE window frame offsets and are applied in their own unit.

DROP TABLE IF EXISTS test_window_interval_offset;
CREATE TABLE test_window_interval_offset (dt Date, ts DateTime('UTC'), val UInt32) ENGINE = MergeTree ORDER BY dt;
INSERT INTO test_window_interval_offset VALUES
    ('2024-01-30', '2024-01-30 00:00:00', 1),
    ('2024-01-31', '2024-01-31 00:30:00', 2),
    ('2024-02-01', '2024-02-01 01:00:00', 3),
    ('2024-02-29', '2024-02-29 00:00:00', 4),
    ('2024-03-01', '2024-03-01 00:00:00', 5),
    ('2024-03-31', '2024-03-31 00:00:00', 6),
    ('2025-01-31', '2025-01-31 00:00:00', 7);

SELECT 'Date key, INTERVAL DAY, begin offset';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'Date key, toIntervalDay function';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN toIntervalDay(1) PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'Date key, INTERVAL WEEK, end offset';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN CURRENT ROW AND INTERVAL 1 WEEK FOLLOWING) AS rolling
FROM test_window_interval_offset;

SELECT 'Date key, INTERVAL MONTH, calendar arithmetic';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'Date key, INTERVAL MONTH following';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN CURRENT ROW AND INTERVAL 1 MONTH FOLLOWING) AS rolling
FROM test_window_interval_offset;

SELECT 'Date key, INTERVAL YEAR';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 YEAR PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'Date key, INTERVAL QUARTER, DESC';
SELECT dt, val, sum(val) OVER (ORDER BY dt DESC RANGE BETWEEN INTERVAL 1 QUARTER PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'Date32 key, INTERVAL MONTH, calendar arithmetic';
SELECT dt32, val, sum(val) OVER (ORDER BY dt32 RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS rolling
FROM (SELECT toDate32(dt) AS dt32, val FROM test_window_interval_offset);

SELECT 'Date32 key, INTERVAL DAY';
SELECT dt32, val, sum(val) OVER (ORDER BY dt32 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS rolling
FROM (SELECT toDate32(dt) AS dt32, val FROM test_window_interval_offset);

SELECT 'DateTime key, INTERVAL HOUR';
SELECT ts, val, sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'DateTime key, INTERVAL DAY';
SELECT ts, val, sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'DateTime key, INTERVAL MONTH is rejected';
SELECT ts, val, sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }

SELECT 'Nullable Date key, INTERVAL MONTH';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS rolling
FROM (SELECT if(val = 4, NULL, dt) AS dt, val FROM test_window_interval_offset);

SELECT 'Nullable Date key, INTERVAL MONTH, NULLS FIRST';
SELECT dt, val, sum(val) OVER (ORDER BY dt NULLS FIRST RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS rolling
FROM (SELECT if(val = 4, NULL, dt) AS dt, val FROM test_window_interval_offset);

-- A month bound may fall outside the range of a Date key, [1970-01-01, 2149-06-06]: it must not be clamped or wrap.
SELECT 'Date key, INTERVAL MONTH bounds below 1970-01-01';
SELECT dt,
    count() OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS month_preceding,
    count() OVER (ORDER BY dt RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 1 MONTH PRECEDING) AS before_month_preceding,
    count() OVER (ORDER BY dt DESC RANGE BETWEEN INTERVAL 1 MONTH FOLLOWING AND UNBOUNDED FOLLOWING) AS desc_after_month_following
FROM (SELECT arrayJoin([toDate('1970-01-01'), toDate('1970-01-20'), toDate('1970-02-15')]) AS dt)
ORDER BY dt;

SELECT 'Date key, INTERVAL MONTH bounds above 2149-06-06';
SELECT dt,
    count() OVER (ORDER BY dt RANGE BETWEEN CURRENT ROW AND INTERVAL 1 MONTH FOLLOWING) AS month_following,
    count() OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH FOLLOWING AND UNBOUNDED FOLLOWING) AS after_month_following,
    count() OVER (ORDER BY dt DESC RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS desc_month_preceding
FROM (SELECT arrayJoin([toDate('2149-05-01'), toDate('2149-05-20'), toDate('2149-06-01')]) AS dt)
ORDER BY dt;

-- The same for a Date32 key, whose range is [0000-01-01, 9999-12-31]: a bound past it is below or above every key.
SELECT 'Date32 key, INTERVAL MONTH and YEAR bounds below 0000-01-01';
SELECT dt,
    count() OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS month_preceding,
    count() OVER (ORDER BY dt RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 1 MONTH PRECEDING) AS before_month_preceding,
    count() OVER (ORDER BY dt DESC RANGE BETWEEN INTERVAL 1 MONTH FOLLOWING AND UNBOUNDED FOLLOWING) AS desc_after_month_following,
    count() OVER (ORDER BY dt RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 1 YEAR PRECEDING) AS before_year_preceding
FROM (SELECT arrayJoin([toDate32('0000-01-01'), toDate32('0000-01-15'), toDate32('0000-02-20')]) AS dt)
ORDER BY dt;

SELECT 'Date32 key, INTERVAL MONTH and YEAR bounds above 9999-12-31';
SELECT dt,
    count() OVER (ORDER BY dt RANGE BETWEEN CURRENT ROW AND INTERVAL 1 MONTH FOLLOWING) AS month_following,
    count() OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH FOLLOWING AND UNBOUNDED FOLLOWING) AS after_month_following,
    count() OVER (ORDER BY dt DESC RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS desc_month_preceding,
    count() OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 YEAR FOLLOWING AND UNBOUNDED FOLLOWING) AS after_year_following
FROM (SELECT arrayJoin([toDate32('9999-11-30'), toDate32('9999-12-15'), toDate32('9999-12-31')]) AS dt)
ORDER BY dt;

SELECT 'Windows differing only by unit are not merged';
SELECT dt, val,
    sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS by_day,
    sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS by_month
FROM test_window_interval_offset;

SELECT 'Named window and view';
SELECT dt, val, sum(val) OVER w AS rolling
FROM test_window_interval_offset
WINDOW w AS (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW);

DROP VIEW IF EXISTS test_window_interval_offset_view;
CREATE VIEW test_window_interval_offset_view AS
    SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS rolling
    FROM test_window_interval_offset;
SELECT * FROM test_window_interval_offset_view;
DROP VIEW test_window_interval_offset_view;

SELECT 'Mixed units, frame in the past';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 WEEK PRECEDING AND INTERVAL 1 DAY PRECEDING) AS rolling
FROM test_window_interval_offset;

SELECT 'Mixed units, frame ends before it starts, empty frames';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 WEEK PRECEDING) AS rolling
FROM test_window_interval_offset;

SELECT 'Mixed units, month vs days, frame is empty only in short months';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 30 DAY PRECEDING) AS rolling
FROM test_window_interval_offset;

SELECT 'Frame description keeps the unit';
SELECT explain FROM (EXPLAIN SELECT sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 2 DAY FOLLOWING) FROM test_window_interval_offset)
WHERE explain LIKE '%Window%';

SELECT 'Rejected: ROWS frame with interval';
SELECT sum(val) OVER (ORDER BY dt ROWS BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }
SELECT 'Rejected: numeric key with interval';
SELECT sum(val) OVER (ORDER BY val RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }
SELECT 'Rejected: sub-day interval on Date key';
SELECT sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }
SELECT 'Rejected: negative interval';
SELECT sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL -1 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }
SELECT 'Rejected: non-constant offset';
SELECT sum(val) OVER (ORDER BY val ROWS BETWEEN val PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }

-- Like a numeric offset, an interval offset that does not fit the type of the key is rejected rather than truncated.
SELECT 'Rejected: interval offset out of the range of the key';
SELECT sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 65536 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT sum(val) OVER (ORDER BY dt RANGE BETWEEN CURRENT ROW AND INTERVAL 9363 WEEK FOLLOWING) FROM test_window_interval_offset; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 49711 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT sum(val) OVER (ORDER BY dt32 RANGE BETWEEN INTERVAL 306783379 WEEK PRECEDING AND CURRENT ROW)
FROM (SELECT toDate32(dt) AS dt32, val FROM test_window_interval_offset); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'Largest interval offsets that fit the key';
SELECT dt, val,
    sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 65535 DAY PRECEDING AND CURRENT ROW) AS date_days,
    sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 49710 DAY PRECEDING AND CURRENT ROW) AS datetime_days
FROM test_window_interval_offset
ORDER BY dt;

DROP TABLE test_window_interval_offset;

-- On a DateTime key in a time zone with DST, `ts - INTERVAL 1 DAY` is not monotone in `ts` around the switch, and the
-- frame bounds only move forward, so INTERVAL DAY and WEEK are rejected there. The rows are in the repeated hour of
-- 2024-10-27 in Berlin: 02:30 CEST (00:30 UTC) is followed by 02:00 CET (01:00 UTC). A day before them is 2024-10-26
-- 02:30 and 02:00, in the opposite order. INTERVAL HOUR frames are exact seconds and stay correct.

DROP TABLE IF EXISTS test_window_interval_offset_dst;
CREATE TABLE test_window_interval_offset_dst (ts DateTime('Europe/Berlin'), val UInt32) ENGINE = MergeTree ORDER BY ts;
INSERT INTO test_window_interval_offset_dst VALUES
    (toDateTime('2024-10-26 00:00:00', 'UTC'), 1),
    (toDateTime('2024-10-26 00:30:00', 'UTC'), 2),
    (toDateTime('2024-10-27 00:30:00', 'UTC'), 3),
    (toDateTime('2024-10-27 01:00:00', 'UTC'), 4);

SELECT 'DST: INTERVAL DAY and WEEK are rejected for a DateTime key in a time zone with DST';
SELECT sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset_dst; -- { serverError BAD_ARGUMENTS }
SELECT sum(val) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND INTERVAL 1 WEEK FOLLOWING) FROM test_window_interval_offset_dst; -- { serverError BAD_ARGUMENTS }
SELECT sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW)
FROM (SELECT if(val = 2, NULL, ts) AS ts, val FROM test_window_interval_offset_dst); -- { serverError BAD_ARGUMENTS }

SELECT 'DST: INTERVAL HOUR frames in the repeated hour';
SELECT ts, toTimezone(ts, 'UTC') AS ts_utc, val,
    sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND CURRENT ROW) AS h24_preceding,
    sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS h1_preceding,
    sum(val) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND INTERVAL 24 HOUR FOLLOWING) AS h24_following
FROM test_window_interval_offset_dst;

DROP TABLE test_window_interval_offset_dst;
