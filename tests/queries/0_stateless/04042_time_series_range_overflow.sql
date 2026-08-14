-- timeSeriesRange should throw on overflow instead of undefined behavior
SELECT timeSeriesRange(toDateTime64(0, 3), toDateTime64(1, 3), 9223372036854775807); -- { serverError DECIMAL_OVERFLOW }
SELECT timeSeriesFromGrid(toDateTime64(0, 3), toDateTime64(1, 3), 9223372036854775807, [1.0, 2.0]); -- { serverError DECIMAL_OVERFLOW }

-- Steps wider than 32 bits must be applied exactly for DateTime/UInt32 timestamps, not truncated to the lower 32 bits.
-- See https://github.com/ClickHouse/ClickHouse/issues/104266
SELECT timeSeriesRange(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 4294967297::Int64);
SELECT timeSeriesRange(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 8589934594::Int64);
SELECT timeSeriesRange(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 4294967296::Int64);
SELECT timeSeriesRange(42::UInt32, 52::UInt32, 4294967297::UInt64);
SELECT timeSeriesFromGrid(toDateTime('2025-01-01 00:00:00', 'UTC'), toDateTime('2025-01-01 00:00:10', 'UTC'), 4294967297::Int64, [42.0]);
