SELECT dateTrunc('DAY', toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc('MONTH', toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc('WEEK', toDate('2022-03-01'));
SELECT dateTrunc('Day', toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc('Month', toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc('Week', toDate('2022-03-01'));
SELECT dateTrunc('day', toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc('month', toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc('week', toDate('2022-03-01'));
SELECT dateTrunc('Nanosecond', toDate('2022-03-01')); -- {  serverError 36 }
SELECT dateTrunc('MicroSecond', toDate('2022-03-01')); -- {  serverError 36 }
SELECT dateTrunc('MILLISECOND', toDate('2022-03-01')); -- {  serverError 36 }
