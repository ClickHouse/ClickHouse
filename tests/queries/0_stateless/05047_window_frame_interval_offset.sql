-- Interval constants are accepted as RANGE window frame offsets and are applied in their own unit.
-- The analyzers differ only in how they record the interval kind in the frame; the old analyzer section repeats the blocks that cover that path and window identity.

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

SET enable_analyzer = 1;

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

SET enable_analyzer = 0;

SELECT 'Old analyzer: Date key, INTERVAL MONTH';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'Old analyzer: DateTime key, INTERVAL HOUR';
SELECT ts, val, sum(val) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'Old analyzer: windows differing only by unit are not merged';
SELECT dt, val,
    sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS by_day,
    sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) AS by_month
FROM test_window_interval_offset;

SELECT 'Old analyzer, rejected: ROWS frame with interval';
SELECT sum(val) OVER (ORDER BY dt ROWS BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }
SELECT 'Old analyzer, rejected: numeric key with interval';
SELECT sum(val) OVER (ORDER BY val RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM test_window_interval_offset; -- { serverError BAD_ARGUMENTS }

DROP TABLE test_window_interval_offset;
