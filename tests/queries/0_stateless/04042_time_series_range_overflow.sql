-- A positive step which is greater than the duration makes a grid consisting of the start timestamp only.
-- This includes steps which overflow UInt64 after rescaling to the result scale.
SELECT timeSeriesRange(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 18446744073709551615::UInt64);
SELECT timeSeriesRange(toDateTime64(0, 0), toDateTime64(1, 0), 9223372036854775807) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesFromGrid(toDateTime64(0, 0), toDateTime64(1, 0), 9223372036854775807, [1.0]) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesRange(toDateTime64(0, 3), toDateTime64(1, 3), 9223372036854775807) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesFromGrid(toDateTime64(0, 3), toDateTime64(1, 3), 9223372036854775807, [1.0]) SETTINGS session_timezone = 'UTC';
SELECT timeSeriesFromGrid(toDateTime64(0, 3), toDateTime64(1, 3), 9223372036854775807, [1.0, 2.0]); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRange(toDateTime64('2025-01-01 00:00:00', 3, 'UTC'), toDateTime64('2025-01-01 00:00:10', 3, 'UTC'), 18446744073709551615::UInt64) SETTINGS session_timezone = 'UTC';

-- Rescaling a timestamp to a bigger scale can overflow Int64 - this is an error.
SELECT timeSeriesRange(toDateTime64('2262-04-12 00:00:00', 0, 'UTC'), toDateTime64('2262-04-12 00:00:00', 0, 'UTC'), CAST(1, 'Decimal64(9)')); -- { serverError DECIMAL_OVERFLOW }

-- Negative steps are rejected.
SELECT timeSeriesRange(toDateTime64(0, 3), toDateTime64(1, 3), -9223372036854775807); -- { serverError BAD_ARGUMENTS }

-- Ranges wider than Int64 are supported.
SELECT timeSeriesRange('1900-01-01 00:00:00'::DateTime64(9, 'UTC'), '2262-04-11 23:47:16.854775806'::DateTime64(9, 'UTC'), 1000000000) SETTINGS session_timezone = 'UTC';

-- Steps wider than 32 bits must be applied exactly for DateTime/UInt32 timestamps, not truncated to the lower 32 bits.
-- See https://github.com/ClickHouse/ClickHouse/issues/104266
SELECT timeSeriesRange(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 4294967297::Int64);
SELECT timeSeriesRange(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 8589934594::Int64);
SELECT timeSeriesRange(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 4294967296::Int64);
SELECT timeSeriesRange(42::UInt32, 52::UInt32, 4294967297::UInt64);
SELECT timeSeriesFromGrid(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 4294967297::Int64, [42.0]);
