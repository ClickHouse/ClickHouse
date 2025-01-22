SELECT '--- Negative DateTime64';
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Year);
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Quarter);
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Month);
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Week);
SELECT toDayOfWeek(toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Week));
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Day);
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Hour);
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Minute);
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55.1234', 4), INTERVAL 1 Second);
SELECT toStartOfInterval(toDateTime64('1960-03-03 12:55:55.1234', 4), INTERVAL 1 Millisecond);

SELECT '--- Negative Date32';
SELECT toStartOfInterval(toDate32('1960-03-03'), INTERVAL 1 Year);
SELECT toStartOfInterval(toDate32('1960-03-03'), INTERVAL 1 Quarter);
SELECT toStartOfInterval(toDate32('1960-03-03'), INTERVAL 1 Month);
SELECT toStartOfInterval(toDate32('1960-03-03'), INTERVAL 1 Week);
SELECT toDayOfWeek(toStartOfInterval(toDate32('1960-03-03'), INTERVAL 1 Week));
SELECT toStartOfInterval(toDate32('1960-03-03'), INTERVAL 1 Day);
