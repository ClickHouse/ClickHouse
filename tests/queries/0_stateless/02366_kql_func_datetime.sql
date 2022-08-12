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
print '-- startofday()'
-- print startofday(datetime(2017-01-01 10:10:17));
print startofday(datetime(2017-01-01 10:10:17), -1);
print startofday(datetime(2017-01-01 10:10:17), 1);
print '-- startofmonth()';
-- print startofmonth(datetime(2017-01-01 10:10:17));
print startofmonth(datetime(2017-01-01 10:10:17), -1);
print startofmonth(datetime(2017-01-01 10:10:17), 1);
print '-- startofweek()'
-- print startofweek(datetime(2017-01-01 10:10:17));
print startofweek(datetime(2017-01-01 10:10:17), -1);
print startofweek(datetime(2017-01-01 10:10:17), 1);
print '-- startofyear()'
-- print startofyear(datetime(2017-01-01 10:10:17));
print startofyear(datetime(2017-01-01 10:10:17), -1);
print startofyear(datetime(2017-01-01 10:10:17), 1);
print '-- unixtime_seconds_todatetime()';
print unixtime_seconds_todatetime(1546300800);
print '-- weekofyear()';
print week_of_year(datetime(2000-01-01));
print '-- monthofyear()'
print monthofyear(datetime(2015-12-31));
print '-- now()';
print getyear(now(-2d))>1900;


