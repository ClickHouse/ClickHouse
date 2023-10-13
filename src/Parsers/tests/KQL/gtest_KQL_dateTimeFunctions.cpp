#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Datetime, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print week_of_year(datetime(2020-12-31))",
            "SELECT toWeek(parseDateTime64BestEffortOrNull('2020-12-31', 9, 'UTC'), 3, 'UTC')"
        },
        {
            "print startofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT parseDateTime64BestEffortOrNull(toString(toStartOfWeek(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'))), 9, 'UTC') + toIntervalWeek(-1)"
        },
        {
            "print startofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT parseDateTime64BestEffortOrNull(toString(toStartOfMonth(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'))), 9, 'UTC') + toIntervalMonth(-1)"
        },
        {
            "print startofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT parseDateTime64BestEffortOrNull(toString(toStartOfDay(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'))), 9, 'UTC') + toIntervalDay(-1)"
        },
        {
            "print startofyear(datetime(2017-01-01 10:10:17), -1)",
            "SELECT parseDateTime64BestEffortOrNull(toString(toStartOfYear(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'), 'UTC')), 9, 'UTC') + toIntervalYear(-1)"
        },
        {
            "print monthofyear(datetime(2015-12-14))",
            "SELECT toMonth(parseDateTime64BestEffortOrNull('2015-12-14', 9, 'UTC'))"
        },
        {
            "print hourofday(datetime(2015-12-14 10:54:00))",
            "SELECT toHour(parseDateTime64BestEffortOrNull('2015-12-14 10:54:00', 9, 'UTC'))"
        },
        {
            "print getyear(datetime(2015-10-12))",
            "SELECT toYear(parseDateTime64BestEffortOrNull('2015-10-12', 9, 'UTC'))"
        },
        {
            "print getmonth(datetime(2015-10-12))",
            "SELECT toMonth(parseDateTime64BestEffortOrNull('2015-10-12', 9, 'UTC'))"
        },
        {
            "print dayofyear(datetime(2015-10-12))",
            "SELECT toDayOfYear(parseDateTime64BestEffortOrNull('2015-10-12', 9, 'UTC'))"
        },
        {
            "print dayofmonth(datetime(2015-10-12))",
            "SELECT toDayOfMonth(parseDateTime64BestEffortOrNull('2015-10-12', 9, 'UTC'))"
        },
        {
            "print unixtime_seconds_todatetime(1546300899)",
            "SELECT if((toTypeName(1546300899) = 'Int64') OR (toTypeName(1546300899) = 'Int32') OR (toTypeName(1546300899) = 'Float64') OR (toTypeName(1546300899) = 'UInt32') OR (toTypeName(1546300899) = 'UInt64'), toDateTime64(1546300899, 9, 'UTC'), toDateTime64(throwIf(true, 'unixtime_seconds_todatetime only accepts Int, Long and double type of arguments'), 9, 'UTC'))"
        },
        {
            "print dayofweek(datetime(2015-12-20))",
            "SELECT concat(CAST(toDayOfWeek(parseDateTime64BestEffortOrNull('2015-12-20', 9, 'UTC')) % 7, 'String'), '.00:00:00')"
        },
        {
            "print now()",
            "SELECT now64(9, 'UTC')"
        },
        {
            "print now(1d)",
            "SELECT now64(9, 'UTC') + 86400"
        },
        {
            "print ago(2d)",
            "SELECT now64(9, 'UTC') - 172800"
        },
        {
            "print endofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofday(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (((toDateTime(toLastDayOfMonth(toDateTime(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'), 9, 'UTC') + toIntervalMonth(-1)), 9, 'UTC') + toIntervalHour(23)) + toIntervalMinute(59)) + toIntervalSecond(60)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (((toDateTime(toLastDayOfMonth(toDateTime(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'), 9, 'UTC') + toIntervalMonth(1)), 9, 'UTC') + toIntervalHour(23)) + toIntervalMinute(59)) + toIntervalSecond(60)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofyear(datetime(2017-01-01 10:10:17), -1) ",
            "SELECT (((toDateTime(toString(toLastDayOfMonth((toDateTime(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'), 9, 'UTC') + toIntervalYear(-1)) + toIntervalMonth(12 - toInt8(substring(toString(toDateTime(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'), 9, 'UTC')), 6, 2))))), 9, 'UTC') + toIntervalHour(23)) + toIntervalMinute(59)) + toIntervalSecond(60)) - toIntervalMicrosecond(1)"
        },
        {
           "print endofyear(datetime(2017-01-01 10:10:17), 1)" ,
           "SELECT (((toDateTime(toString(toLastDayOfMonth((toDateTime(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'), 9, 'UTC') + toIntervalYear(1)) + toIntervalMonth(12 - toInt8(substring(toString(toDateTime(parseDateTime64BestEffortOrNull('2017-01-01 10:10:17', 9, 'UTC'), 9, 'UTC')), 6, 2))))), 9, 'UTC') + toIntervalHour(23)) + toIntervalMinute(59)) + toIntervalSecond(60)) - toIntervalMicrosecond(1)"
        },
        {
            "print make_datetime(2017,10,01)",
            "SELECT makeDateTime64(2017, 10, 1, 0, 0, 0, 0, 7, 'UTC')"
        },
        {
            "print make_datetime(2017,10,01,12,10)",
            "SELECT makeDateTime64(2017, 10, 1, 12, 10, 0, 0, 7, 'UTC')"
        },
        {
            "print make_datetime(2017,10,01,12,11,0.1234567)",
            "SELECT makeDateTime64(2017, 10, 1, 12, 11, 0.1234567, 0, 7, 'UTC')"
        },
        {
            "print unixtime_microseconds_todatetime(1546300800000000)",
            "SELECT fromUnixTimestamp64Micro(1546300800000000, 'UTC')"
        },
        {
            "print unixtime_milliseconds_todatetime(1546300800000)",
            "SELECT fromUnixTimestamp64Milli(1546300800000, 'UTC')"
        },
        {
            "print unixtime_nanoseconds_todatetime(1546300800000000000)",
            "SELECT fromUnixTimestamp64Nano(1546300800000000000, 'UTC')"
        },
        {
            "print datetime_diff('year',datetime(2017-01-01),datetime(2000-12-31))",
            "SELECT dateDiff('year', parseDateTime64BestEffortOrNull('2017-01-01', 9, 'UTC'), parseDateTime64BestEffortOrNull('2000-12-31', 9, 'UTC')) * -1"
        },
        {
            "print datetime_diff('minute',datetime(2017-10-30 23:05:01),datetime(2017-10-30 23:00:59))",
            "SELECT dateDiff('minute', parseDateTime64BestEffortOrNull('2017-10-30 23:05:01', 9, 'UTC'), parseDateTime64BestEffortOrNull('2017-10-30 23:00:59', 9, 'UTC')) * -1"
        },
        {
            "print datetime(null)",
            "SELECT NULL"
        },
        {
            "print datetime('2014-05-25T08:20:03.123456Z')",
            "SELECT parseDateTime64BestEffortOrNull('2014-05-25T08:20:03.123456Z', 9, 'UTC')"
        },
        {
            "print datetime(2015-12-14 18:54)",
            "SELECT parseDateTime64BestEffortOrNull('2015-12-14 18:54', 9, 'UTC')"
        },
        {
            "print make_timespan(67,12,30,59.9799)",
            "SELECT CONCAT('67.', toString(substring(toString(toTime(parseDateTime64BestEffortOrNull('0000-00-00 12:30:59.9799', 9, 'UTC'))), 12)))"
        },
        {
            "print  todatetime('2014-05-25T08:20:03.123456Z')",
            "SELECT parseDateTime64BestEffortOrNull(toString('2014-05-25T08:20:03.123456Z'), 9, 'UTC')"
        },
        {
            "print format_datetime(todatetime('2009-06-15T13:45:30.6175425'), 'yy-M-dd [H:mm:ss.fff]')",
            "SELECT concat(substring(toString(formatDateTime(parseDateTime64BestEffortOrNull(toString('2009-06-15T13:45:30.6175425'), 9, 'UTC'), '%y-%m-%d [%H:%i:%S.]')), 1, position(toString(formatDateTime(parseDateTime64BestEffortOrNull(toString('2009-06-15T13:45:30.6175425'), 9, 'UTC'), '%y-%m-%d [%H:%i:%S.]')), '.')), substring(substring(toString(parseDateTime64BestEffortOrNull(toString('2009-06-15T13:45:30.6175425'), 9, 'UTC')), position(toString(parseDateTime64BestEffortOrNull(toString('2009-06-15T13:45:30.6175425'), 9, 'UTC')), '.') + 1), 1, 3), substring(toString(formatDateTime(parseDateTime64BestEffortOrNull(toString('2009-06-15T13:45:30.6175425'), 9, 'UTC'), '%y-%m-%d [%H:%i:%S.]')), position(toString(formatDateTime(parseDateTime64BestEffortOrNull(toString('2009-06-15T13:45:30.6175425'), 9, 'UTC'), '%y-%m-%d [%H:%i:%S.]')), '.') + 1, length(toString(formatDateTime(parseDateTime64BestEffortOrNull(toString('2009-06-15T13:45:30.6175425'), 9, 'UTC'), '%y-%m-%d [%H:%i:%S.]')))))"
        },
        {
            "print format_datetime(datetime(2015-12-14 02:03:04.12345), 'y-M-d h:m:s tt')",
            "SELECT formatDateTime(parseDateTime64BestEffortOrNull('2015-12-14 02:03:04.12345', 9, 'UTC'), '%y-%m-%e %h:%i:%S %p')"
        },
        {
            "print format_timespan(time(1d), 'd-[hh:mm:ss]')",
            "SELECT concat(leftPad('1', 1, '0'), toString(formatDateTime(toDateTime64(CAST('86400', 'Float64'), 9, 'UTC'), '-[%h:%i:%S]')))"
        },
        {
            "print format_timespan(time('12:30:55.123'), 'ddddd-[hh:mm:ss.ffff]')",
            "SELECT concat(leftPad('0', 5, '0'), substring(toString(formatDateTime(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC'), '-[%H:%i:%S.]')), 1, length(toString(formatDateTime(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC'), '-[%H:%i:%S.]'))) - position(reverse(toString(formatDateTime(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC'), '-[%H:%i:%S.]'))), ']')), substring(substring(toString(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC')), position(toString(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC')), '.') + 1), 1, 4), substring(toString(formatDateTime(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC'), '-[%H:%i:%S.]')), position(toString(formatDateTime(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC'), '-[%H:%i:%S.]')), ']'), length(toString(formatDateTime(toDateTime64(CAST('45055.123', 'Float64'), 9, 'UTC'), '-[%H:%i:%S.]')))))"
        },
        {
            "print v1=format_timespan(time('29.09:00:05.12345'), 'dd.hh:mm:ss:FF')",
            "SELECT concat(leftPad('29', 2, '0'), substring(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S:')), 1, (length(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S:'))) - position(reverse(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S:'))), ':')) + 1), substring(substring(toString(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC')), position(toString(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC')), '.') + 1), 1, 2)) AS v1"
        },
        {
            "print v2=format_timespan(time('29.09:00:05.12345'), 'ddd.h:mm:ss [fffffff]');",
            "SELECT concat(leftPad('29', 3, '0'), substring(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S []')), 1, length(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S []'))) - position(reverse(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S []'))), ']')), substring(substring(toString(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC')), position(toString(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC')), '.') + 1), 1, 7), substring(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S []')), position(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S []')), ']'), length(toString(formatDateTime(toDateTime64(CAST('2538005.12345', 'Float64'), 9, 'UTC'), '.%H:%i:%S []'))))) AS v2"
        },
        {
            "print datetime_part('day', datetime(2017-10-30 01:02:03.7654321))",
            "SELECT formatDateTime(parseDateTime64BestEffortOrNull('2017-10-30 01:02:03.7654321', 9, 'UTC'), '%e')"
        },
        {
            "print datetime_add('day',1,datetime(2017-10-30 01:02:03.7654321))",
            "SELECT parseDateTime64BestEffortOrNull('2017-10-30 01:02:03.7654321', 9, 'UTC') + toIntervalDay(1)"
        },
        {
            "print totimespan(time(1d))",
            "SELECT CAST('86400', 'Float64')"
        },
        {
            "print totimespan('0.01:34:23')",
            "SELECT CAST('5663', 'Float64')"
        },
        {
            "print totimespan(time('-1:12:34'))",
            "SELECT CAST('-4354', 'Float64')"
        },
        {
            "print totimespan(-1d)",
            "SELECT -86400"
        },
        {
            "print totimespan('abc')",
            "SELECT NULL"
        },
        {
            "print time(2)",
            "SELECT CAST('172800', 'Float64')"
        },
        {
            "hits | project bin(datetime(EventTime), 1m)",
            "SELECT toDateTime64(toInt64(toFloat64(if((toTypeName(EventTime) = 'Int64') OR (toTypeName(EventTime) = 'Int32') OR (toTypeName(EventTime) = 'Float64') OR (toTypeName(EventTime) = 'UInt32') OR (toTypeName(EventTime) = 'UInt64'), toDateTime64(EventTime, 9, 'UTC'), parseDateTime64BestEffortOrNull(CAST(EventTime, 'String'), 9, 'UTC'))) / 60) * 60, 9, 'UTC')\nFROM hits"
        }

})));
