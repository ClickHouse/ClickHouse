SET session_timezone = 'UTC';

SELECT dateTrunc('Second', toDateTime64('1955-03-01 12:55:55', 2));
SELECT dateTrunc('Minute', toDateTime64('1955-03-01 12:55:55', 2));
SELECT dateTrunc('Hour', toDateTime64('1960-03-01 12:55:55', 2));
SELECT dateTrunc('Day', toDateTime64('1950-03-01 12:55:55', 2));
SELECT dateTrunc('Week', toDateTime64('1960-03-01 12:55:55', 2));
SELECT dateTrunc('Month', toDateTime64('1950-03-01 12:55:55', 2));
SELECT dateTrunc('Year', toDateTime64('1955-03-01 12:55:55', 2));
