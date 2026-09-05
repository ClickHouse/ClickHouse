SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30);
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:01.000'::DateTime64(3), '0.10'::Decimal64(3));
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:30.000'::DateTime64(3), 30);
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), 30);
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), 0);

-- Different scales
SELECT timeSeriesRange('2025-06-01 00:00:00.0'::DateTime64(1), '2025-06-01 00:00:01.00'::DateTime64(2), '0.123'::Decimal64(3));

-- Wrong range: end_timestamp < start_timestamp
SELECT timeSeriesRange('2025-06-01 00:01:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), 30); -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRange('2025-06-01 00:01:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), -30); -- {serverError BAD_ARGUMENTS}

-- Wrong step
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:50.000'::DateTime64(3), 0); -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:50.000'::DateTime64(3), -10); -- {serverError BAD_ARGUMENTS}

-- timeSeriesFromGrid without NULLs
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [100, 200, 300, 400]);

-- timeSeriesFromGrid with NULLs
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [100, 200, NULL, 400]);
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:03:00.000'::DateTime64(3), 30, [100, NULL, 300, NULL, NULL, 600, NULL]);
SELECT timeSeriesFromGrid(toDateTime64(0, 3, 'UTC'), toDateTime64(3, 3, 'UTC'), 1, if(number = 0, [1, 2, 3, 4], [1, NULL, 3, 4])) FROM numbers(2) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesFromGrid(toDateTime64(0, 3, 'UTC'), toDateTime64(3, 3, 'UTC'), 1, if(number = 0, [1, NULL, 3, 4], [5, 6, 7, 8])) FROM numbers(2) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesFromGrid(toDateTime64(0, 3, 'UTC'), toDateTime64(3, 3, 'UTC'), 1, [NULL, NULL, NULL, NULL]);

-- Wrong number of values
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [10, 20, 30]); -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [10, 20, 30, 40, 50]); -- {serverError BAD_ARGUMENTS}

-- Timestamps before 1970 (negative raw values of DateTime64)
SELECT timeSeriesRange(CAST(-60, 'DateTime64(3)'), CAST(120, 'DateTime64(3)'), 60) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesRange(CAST(-0.5, 'DateTime64(3)'), CAST(1, 'DateTime64(3)'), '0.5'::Decimal64(3)) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesFromGrid(CAST(-60, 'DateTime64(3)'), CAST(120, 'DateTime64(3)'), 60, [1, NULL, 3, 4]) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesFromGrid(CAST(-60, 'DateTime64(3)'), CAST(120, 'DateTime64(3)'), 60, [1, 2, 3, 4]) SETTINGS session_timezone = 'UTC';
