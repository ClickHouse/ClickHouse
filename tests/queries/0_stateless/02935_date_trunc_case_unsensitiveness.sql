SELECT dateTrunc('DAY', toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc('MONTH', toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc('WEEK', toDate('2022-03-01'));
SELECT dateTrunc('Day', toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc('Month', toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc('Week', toDate('2022-03-01'));
SELECT dateTrunc('day', toDateTime('2022-03-01 12:55:55'));
SELECT dateTrunc('month', toDateTime64('2022-03-01 12:55:55', 2));
SELECT dateTrunc('week', toDate('2022-03-01'));
SELECT dateTrunc('Nanosecond', toDateTime64('2022-03-01 12:12:12.0123', 3));
SELECT dateTrunc('MicroSecond', toDateTime64('2022-03-01 12:12:12.0123456', 7));
SELECT dateTrunc('MILLISECOND', toDateTime64('2022-03-01 12:12:12.012324251', 9));
SELECT dateTrunc('mICROsECOND', toDateTime64('2022-03-01 12:12:12.0123', 4));
SELECT dateTrunc('mIllISecoNd', toDateTime64('2022-03-01 12:12:12.0123456', 6));
SELECT dateTrunc('NANoSecoND', toDateTime64('2022-03-01 12:12:12.012345678', 8));
SELECT dateTrunc('Nanosecond', toDateTime64('1950-03-01 12:12:12.0123', 3));
SELECT dateTrunc('MicroSecond', toDateTime64('1951-03-01 12:12:12.0123456', 7));
SELECT dateTrunc('MILLISECOND', toDateTime64('1952-03-01 12:12:12.012324251', 9));
SELECT dateTrunc('mICROsECOND', toDateTime64('1965-03-01 12:12:12.0123', 4));
SELECT dateTrunc('mIllISecoNd', toDateTime64('1966-03-01 12:12:12.0123456', 6));
SELECT dateTrunc('NANoSecoND', toDateTime64('1967-03-01 12:12:12.012345678', 8));
SELECT dateTrunc('Nanosecond', toDateTime('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc('MicroSecond', toDateTime('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc('MILLISECOND', toDateTime('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc('Nanosecond', toDate('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc('MicroSecond', toDate('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTrunc('MILLISECOND', toDate('2022-03-01')); -- {  serverError ILLEGAL_TYPE_OF_ARGUMENT }
