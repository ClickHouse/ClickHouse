SET sql_compatible_division_for_integers = 1;
SELECT kqlDivide(toIntervalMillisecond(15), toIntervalMillisecond(10));
SELECT kqlDivide(materialize(toIntervalHour(3)), materialize(toIntervalMinute(120)));
SELECT kqlDivide(toIntervalMillisecond(15), toIntervalMillisecond(0));
SELECT kqlDivide(CAST(NULL, 'Nullable(IntervalMillisecond)'), toIntervalMillisecond(10));

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';
print 15ms / 10ms, 3h / 120m, 7 / 2, 7.0 / 2;
datatable (a:timespan, b:timespan) [15ms, 10ms, 3h, 120m] | project r = a / b;
print unixtime_milliseconds_todatetime(1500), unixtime_microseconds_todatetime(1500000), unixtime_nanoseconds_todatetime(1500000000);
datatable (n:long) [1500, 1750] | project r = unixtime_milliseconds_todatetime(n);
SET dialect = 'clickhouse';

SET sql_compatible_division_for_integers = 0;
SELECT kqlDivide(toIntervalMillisecond(15), toIntervalMillisecond(10));
SET dialect = 'kusto';
print unixtime_milliseconds_todatetime(1500), unixtime_microseconds_todatetime(1500000), unixtime_nanoseconds_todatetime(1500000000);
SET dialect = 'clickhouse';
