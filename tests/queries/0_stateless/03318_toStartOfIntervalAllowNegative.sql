-- Works just like the toStartOfInterval so no need to test meticulously
SELECT '--- Negative DateTime64';
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Year);
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Quarter);
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Month);
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Week);
SELECT toDayOfWeek(toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Week));
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Day);
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Hour);
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Minute);
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55.1234', 4), INTERVAL 1 Second);
SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55.1234', 4), INTERVAL 1 Millisecond);

SELECT '--- Negative Date32';
SELECT toStartOfIntervalAllowNegative(toDate32('1960-03-03'), INTERVAL 1 Year);
SELECT toStartOfIntervalAllowNegative(toDate32('1960-03-03'), INTERVAL 1 Quarter);
SELECT toStartOfIntervalAllowNegative(toDate32('1960-03-03'), INTERVAL 1 Month);
SELECT toStartOfIntervalAllowNegative(toDate32('1960-03-03'), INTERVAL 1 Week);
SELECT toDayOfWeek(toStartOfIntervalAllowNegative(toDate32('1960-03-03'), INTERVAL 1 Week));
SELECT toStartOfIntervalAllowNegative(toDate32('1960-03-03'), INTERVAL 1 Day);
