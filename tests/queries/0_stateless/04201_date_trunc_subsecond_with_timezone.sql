-- Test: exercises `dateTrunc` 3-argument form (with timezone) for sub-second
-- units (millisecond, microsecond, nanosecond). Also verifies the return
-- type scale (3, 6, 9). The PR that added these units only tested the
-- 2-argument form; the 3-argument timezone path and scale-selection logic
-- are unexercised across the test suite.
-- Covers: src/Functions/date_trunc.cpp:209-216 — DateTime64 result branch
-- with timezone extraction and scale selection (3 / 6 / 9).
SET session_timezone = 'UTC';

-- Scale verification for the 2-argument form (output type only)
SELECT toTypeName(dateTrunc('millisecond', toDateTime64('2022-03-01 12:12:12.012345678', 9)));
SELECT toTypeName(dateTrunc('microsecond', toDateTime64('2022-03-01 12:12:12.012345678', 9)));
SELECT toTypeName(dateTrunc('nanosecond',  toDateTime64('2022-03-01 12:12:12.012345678', 9)));

-- 3-argument form: timezone propagation into the DateTime64 result type
SELECT toTypeName(dateTrunc('millisecond', toDateTime64('2022-03-01 12:12:12.012345678', 9, 'UTC'), 'Asia/Tokyo'));
SELECT toTypeName(dateTrunc('microsecond', toDateTime64('2022-03-01 12:12:12.012345678', 9, 'UTC'), 'Asia/Tokyo'));
SELECT toTypeName(dateTrunc('nanosecond',  toDateTime64('2022-03-01 12:12:12.012345678', 9, 'UTC'), 'Asia/Tokyo'));

-- 3-argument form: timezone shifts the displayed value (UTC -> JST is +09:00)
SELECT dateTrunc('millisecond', toDateTime64('2022-03-01 12:12:12.012345678', 9, 'UTC'), 'Asia/Tokyo');
SELECT dateTrunc('microsecond', toDateTime64('2022-03-01 12:12:12.012345678', 9, 'UTC'), 'Asia/Tokyo');
SELECT dateTrunc('nanosecond',  toDateTime64('2022-03-01 12:12:12.012345678', 9, 'UTC'), 'Asia/Tokyo');
