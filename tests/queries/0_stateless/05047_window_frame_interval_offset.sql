-- Interval constants must be accepted as window frame offsets.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_window_interval_offset;
CREATE TABLE test_window_interval_offset (dt Date, val UInt32) ENGINE = MergeTree ORDER BY dt;
INSERT INTO test_window_interval_offset VALUES ('2024-01-01', 10), ('2024-01-02', 20), ('2024-01-03', 30), ('2024-01-04', 40), ('2024-01-05', 50);

SELECT 'INTERVAL syntax, begin offset';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'toIntervalDay function, begin offset';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN toIntervalDay(2) PRECEDING AND CURRENT ROW) AS rolling
FROM test_window_interval_offset;

SELECT 'INTERVAL syntax, end offset';
SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN CURRENT ROW AND INTERVAL 1 DAY FOLLOWING) AS rolling
FROM test_window_interval_offset;

SELECT 'named window';
SELECT dt, val, sum(val) OVER w AS rolling
FROM test_window_interval_offset
WINDOW w AS (ORDER BY dt RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW);

SELECT 'view with INTERVAL window frame';
DROP VIEW IF EXISTS test_window_interval_offset_view;
CREATE VIEW test_window_interval_offset_view AS
    SELECT dt, val, sum(val) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW) AS rolling
    FROM test_window_interval_offset;
SELECT * FROM test_window_interval_offset_view;
DROP VIEW test_window_interval_offset_view;

SELECT 'ROWS frame with INTERVAL, old analyzer parity';
SELECT number, sum(number) OVER (ORDER BY number ROWS BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW) AS rolling
FROM numbers(5);

SELECT 'non-constant offset is still rejected';
SELECT number, sum(number) OVER (ORDER BY number ROWS BETWEEN number PRECEDING AND CURRENT ROW)
FROM numbers(5); -- { serverError BAD_ARGUMENTS }

DROP TABLE test_window_interval_offset;
