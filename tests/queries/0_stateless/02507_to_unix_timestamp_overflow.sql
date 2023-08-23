SELECT toUnixTimestamp(toDateTime64('1928-12-31 12:12:12.123', 3, 'UTC')); -- { serverError DECIMAL_OVERFLOW }
SELECT toInt64(toDateTime64('1928-12-31 12:12:12.123', 3, 'UTC'));
