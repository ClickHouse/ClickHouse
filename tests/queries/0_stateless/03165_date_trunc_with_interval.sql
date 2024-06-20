SELECT dateTrunc(INTERVAL 2 DAY, toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc(INTERVAL 2 MONTH, toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc(INTERVAL 2 WEEK, toDate('2022-03-01'));
SELECT dateTrunc(INTERVAL 2 day, toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc(INTERVAL 2 month, toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc(INTERVAL 2 week, toDate('2022-03-01'));
SELECT dateTrunc(INTERVAL 2 day, toDate('2022-03-01'));
SELECT dateTrunc(INTERVAL 2 day, toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc(INTERVAL 2 month, toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc(INTERVAL 2 week, toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc(INTERVAL 3 HOUR, toDateTime64('2022-03-01 12:12:12.0123', 3));
SELECT dateTrunc(INTERVAL 3 MINUTE, toDateTime64('2022-03-01 12:12:12.0123', 3));
SELECT dateTrunc(INTERVAL 3 SECOND, toDateTime64('2022-03-01 12:12:14.0123456', 7));
SELECT dateTrunc(INTERVAL 2 hour, toDateTime64('2022-03-01 12:12:13.012324251', 9));
SELECT dateTrunc(INTERVAL 2 minute, toDateTime64('2022-03-01 12:12:13.012324251', 9));
SELECT dateTrunc(INTERVAL 2 second, toDateTime64('2022-03-01 12:12:12.012324251', 9));
SELECT dateTrunc(INTERVAL 1 year, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 2 year, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 5 year, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 1 quarter, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 2 quarter, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 3 quarter, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 1 month, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 2 month, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 5 month, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 1 week, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 2 week, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 3 day, toDateTime(1549483055), 'Asia/Istanbul');
SELECT dateTrunc(INTERVAL 2 hour, toDate('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc(INTERVAL 2 minute, toDate('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc(INTERVAL 2 second, toDate('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc(INTERVAL 6 hour, toDate(1549483055), 'Asia/Istanbul'); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
