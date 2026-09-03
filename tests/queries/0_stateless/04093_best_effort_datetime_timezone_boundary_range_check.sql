-- Tags: no-fasttest
-- Test for missing range check after adjust_time_zone in parseDateTimeBestEffortImpl
-- https://github.com/ClickHouse/ClickHouse/issues/102601

SET session_timezone = 'UTC';
SET date_time_input_format = 'best_effort';

-- Upper boundary: 2106-02-07 06:28:15 UTC is exactly UINT32_MAX.
-- With -01:00 offset, UTC time is 2106-02-07 07:28:15, which exceeds UINT32_MAX.
-- Should be inferred as DateTime64, not DateTime with wrap-around.
SELECT d, toTypeName(d) FROM format(JSONEachRow, '{"d" : "2106-02-07 06:28:15-01:00"}');

-- Lower boundary: 1970-01-01 00:00:00 UTC is timestamp 0.
-- With +01:00 offset, UTC time is 1969-12-31 23:00:00, which is negative.
-- Should be inferred as DateTime64, not DateTime with clamped value.
SELECT d, toTypeName(d) FROM format(JSONEachRow, '{"d" : "1970-01-01 00:00:00+01:00"}');

-- Control: values that SHOULD remain DateTime (timezone adjustment stays within range)
SELECT d, toTypeName(d) FROM format(JSONEachRow, '{"d" : "2106-02-07 06:28:15+01:00"}');
SELECT d, toTypeName(d) FROM format(JSONEachRow, '{"d" : "1970-01-01 01:00:00+01:00"}');
