#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print week_of_year(datetime(2020-12-31))",
            "SELECT toWeek(toDateTime64('2020-12-31', 9, 'UTC'), 3, 'UTC')"
        },
        {
            "print startofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfWeek(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(-1)"
        },
        {
            "print startofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfMonth(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalMonth(-1)"
        },
        {
            "print startofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(-1)"

        },
        {
            "print startofyear(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfYear(toDateTime64('2017-01-01 10:10:17', 9, 'UTC'), 'UTC'), 9, 'UTC') + toIntervalYear(-1)"
        },
        {
            "print monthofyear(datetime(2015-12-14))",
            "SELECT toMonth(toDateTime64('2015-12-14', 9, 'UTC'))"
        },
        {
            "print hourofday(datetime(2015-12-14 10:54:00))",
            "SELECT toHour(toDateTime64('2015-12-14 10:54:00', 9, 'UTC'))"
        },
        {
            "print getyear(datetime(2015-10-12))",
            "SELECT toYear(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print getmonth(datetime(2015-10-12))",
            "SELECT toMonth(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print dayofyear(datetime(2015-10-12))",
            "SELECT toDayOfYear(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print dayofmonth(datetime(2015-10-12))",
            "SELECT toDayOfMonth(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print unixtime_seconds_todatetime(1546300899)",
            "SELECT toDateTime64(1546300899, 9, 'UTC')"
        },
        {
            "print dayofweek(datetime(2015-12-20))",
            "SELECT toDayOfWeek(toDateTime64('2015-12-20', 9, 'UTC')) % 7"
        },
        {
            "print now()",
            "SELECT now64(9, 'UTC')"
        },
        {

            "print now(1d)",
            "SELECT now64(9, 'UTC') + 86400."
        },
        {
            "print ago(2d)",
            "SELECT now64(9, 'UTC') - 172800."
        },  
        {
            "print endofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofday(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(1 + 1)) - toIntervalMicrosecond(1)"

        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalMonth(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalMonth(1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofyear(datetime(2017-01-01 10:10:17), -1) ",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalYear(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
           "print endofyear(datetime(2017-01-01 10:10:17), 1)" ,
           "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalYear(1 + 1)) - toIntervalMicrosecond(1)"
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
            "SELECT dateDiff('year', toDateTime64('2017-01-01', 9, 'UTC'), toDateTime64('2000-12-31', 9, 'UTC')) * -1"
        },
        {
            "print datetime_diff('minute',datetime(2017-10-30 23:05:01),datetime(2017-10-30 23:00:59))",
            "SELECT dateDiff('minute', toDateTime64('2017-10-30 23:05:01', 9, 'UTC'), toDateTime64('2017-10-30 23:00:59', 9, 'UTC')) * -1"
        }

})));   
