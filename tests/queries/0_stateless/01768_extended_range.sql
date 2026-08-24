SELECT toYear(toDateTime64('1968-12-12 11:22:33', 0, 'UTC'));
SELECT toInt16(toRelativeWeekNum(toDateTime64('1960-11-30 18:00:11.999', 3, 'UTC')));
SELECT toStartOfQuarter(toDateTime64('1990-01-04 12:14:12', 0, 'UTC'));
SELECT toUnixTimestamp(toDateTime64('1900-12-12 11:22:33', 0, 'UTC')); -- { serverError DECIMAL_OVERFLOW }
