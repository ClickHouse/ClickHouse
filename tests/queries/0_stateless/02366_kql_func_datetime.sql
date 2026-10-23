set allow_experimental_kusto_dialect=1;
set dialect = 'kusto';

print '-- dayofmonth()';
print dayofmonth(datetime(2015-12-31));
print '-- dayofweek()';
print dayofweek(datetime(2015-12-31));
print '-- dayofyear()';
print dayofyear(datetime(2015-12-31));
print '-- getmonth()';
print getmonth(datetime(2015-10-12));
print '-- getyear()';
print getyear(datetime(2015-10-12));
print '-- hoursofday()';
print hourofday(datetime(2015-12-31 23:59:59.9));
print '-- startofday()';
print startofday(datetime(2017-01-01 10:10:17));
print startofday(datetime(2017-01-01 10:10:17), -1);
print startofday(datetime(2017-01-01 10:10:17), 1);
print '-- endofday()';
print endofday(datetime(2017-01-01 10:10:17));
print endofday(datetime(2017-01-01 10:10:17), -1);
print endofday(datetime(2017-01-01 10:10:17), 1);
print '-- endofmonth()';
print endofmonth(datetime(2017-01-01 10:10:17));
print endofmonth(datetime(2017-01-01 10:10:17), -1);
print endofmonth(datetime(2017-01-01 10:10:17), 1);
print endofmonth(datetime(2022-09-23));
print '-- startofweek()';
print startofweek(datetime(2017-01-01 10:10:17));
print startofweek(datetime(2017-01-01 10:10:17), -1);
print startofweek(datetime(2017-01-01 10:10:17), 1);
print '-- endofweek()';
print endofweek(datetime(2017-01-01 10:10:17));
print endofweek(datetime(2017-01-01 10:10:17), -1);
print endofweek(datetime(2017-01-01 10:10:17), 1);
print '-- startofyear()';
print startofyear(datetime(2017-01-01 10:10:17));
print startofyear(datetime(2017-01-01 10:10:17), -1);
print startofyear(datetime(2017-01-01 10:10:17), 1);
print '-- endofyear()';
print endofyear(datetime(2017-01-01 10:10:17));
print endofyear(datetime(2017-01-01 10:10:17), -1);
print endofyear(datetime(2017-01-01 10:10:17), 1);
print '-- unixtime_seconds_todatetime()';
print unixtime_seconds_todatetime(1546300800);
print unixtime_seconds_todatetime(1d);
print unixtime_seconds_todatetime(-1d);
print '-- unixtime_microseconds_todatetime';
print unixtime_microseconds_todatetime(1546300800000000);
print '-- unixtime_milliseconds_todatetime()';
print unixtime_milliseconds_todatetime(1546300800000);
print '-- unixtime_nanoseconds_todatetime()';
print unixtime_nanoseconds_todatetime(1546300800000000000);
print '-- weekofyear()';
print week_of_year(datetime(2000-01-01));
print '-- monthofyear()';
print monthofyear(datetime(2015-12-31));
print '-- weekofyear()';
print week_of_year(datetime(2000-01-01));
print '-- now()';
print getyear(now(-2d))>1900;
print '-- make_datetime()';
print make_datetime(2017,10,01,12,10) == datetime(2017-10-01 12:10:00);
print year_month_day_hour_minute = make_datetime(2017,10,01,12,10);
print year_month_day_hour_minute_second = make_datetime(2017,10,01,12,11,0.1234567);
print '-- format_datetime';
print format_datetime(datetime(2015-12-14 02:03:04.12345), 'y-M-d h:m:s.fffffff');
print v1=format_datetime(datetime(2017-01-29 09:00:05),'yy-MM-dd [HH:mm:ss]'), v2=format_datetime(datetime(2017-01-29 09:00:05), 'yyyy-M-dd [H:mm:ss]'), v3=format_datetime(datetime(2017-01-29 09:00:05), 'yy-MM-dd [hh:mm:ss tt]');
print '-- format_timespan()';
print format_timespan(time('14.02:03:04.12345'), 'h:m:s.fffffff');
print v1=format_timespan(time('29.09:00:05.12345'), 'dd.hh:mm:ss:FF');
-- print v2=format_timespan(time('29.09:00:05.12345'), 'ddd.h:mm:ss [fffffff]'); == '029.9:00:05 [1234500]'
print '-- ago()';
-- print ago(1d) - now();
print '-- datetime_diff()';
print year = datetime_diff('year',datetime(2017-01-01),datetime(2000-12-31)), quarter = datetime_diff('quarter',datetime(2017-07-01),datetime(2017-03-30)), month = datetime_diff('month',datetime(2017-01-01),datetime(2015-12-30)), week = datetime_diff('week',datetime(2017-10-29 00:00),datetime(2017-09-30 23:59)), day = datetime_diff('day',datetime(2017-10-29 00:00),datetime(2017-09-30 23:59)), hour = datetime_diff('hour',datetime(2017-10-31 01:00),datetime(2017-10-30 23:59)), minute = datetime_diff('minute',datetime(2017-10-30 23:05:01),datetime(2017-10-30 23:00:59)), second = datetime_diff('second',datetime(2017-10-30 23:00:10.100),datetime(2017-10-30 23:00:00.900));
-- millisecond = datetime_diff('millisecond',datetime(2017-10-30 23:00:00.200100),datetime(2017-10-30 23:00:00.100900)),
-- microsecond = datetime_diff('microsecond',datetime(2017-10-30 23:00:00.1009001),datetime(2017-10-30 23:00:00.1008009)),
-- nanosecond = datetime_diff('nanosecond',datetime(2017-10-30 23:00:00.0000000),datetime(2017-10-30 23:00:00.0000007))
print '-- datetime_part()';
print year = datetime_part("year", datetime(2017-10-30 01:02:03.7654321)),quarter = datetime_part("quarter", datetime(2017-10-30 01:02:03.7654321)),month = datetime_part("month", datetime(2017-10-30 01:02:03.7654321)),weekOfYear = datetime_part("week_of_year", datetime(2017-10-30 01:02:03.7654321)),day = datetime_part("day", datetime(2017-10-30 01:02:03.7654321)),dayOfYear = datetime_part("dayOfYear", datetime(2017-10-30 01:02:03.7654321)),hour = datetime_part("hour", datetime(2017-10-30 01:02:03.7654321)),minute = datetime_part("minute", datetime(2017-10-30 01:02:03.7654321)),second = datetime_part("second", datetime(2017-10-30 01:02:03.7654321));
-- millisecond = datetime_part("millisecond", dt),
-- microsecond = datetime_part("microsecond", dt),
-- nanosecond = datetime_part("nanosecond", dt)
print '-- datetime_add()';
print  year = datetime_add('year',1,make_datetime(2017,1,1)),quarter = datetime_add('quarter',1,make_datetime(2017,1,1)),month = datetime_add('month',1,make_datetime(2017,1,1)),week = datetime_add('week',1,make_datetime(2017,1,1)),day = datetime_add('day',1,make_datetime(2017,1,1)),hour = datetime_add('hour',1,make_datetime(2017,1,1)),minute = datetime_add('minute',1,make_datetime(2017,1,1)),second = datetime_add('second',1,make_datetime(2017,1,1));
