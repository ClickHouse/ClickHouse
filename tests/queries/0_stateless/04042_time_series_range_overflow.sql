-- timeSeriesRange should throw on overflow instead of undefined behavior
SELECT timeSeriesRange(toDateTime64(0, 3), toDateTime64(1, 3), 9223372036854775807); -- { serverError DECIMAL_OVERFLOW }
SELECT timeSeriesFromGrid(toDateTime64(0, 3), toDateTime64(1, 3), 9223372036854775807, [1.0, 2.0]); -- { serverError DECIMAL_OVERFLOW }
