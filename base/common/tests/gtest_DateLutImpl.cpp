#include <common/DateLUT.h>
#include <common/DateLUTImpl.h>

#include <gtest/gtest.h>

#include <string>
#include <cctz/time_zone.h>

/// For the expansion of gtest macros.
#if defined(__clang__)
    #pragma clang diagnostic ignored "-Wused-but-marked-unused"
#endif

// All timezones present at build time and embedded into CH binary.
extern const char * auto_time_zones[];

namespace
{

cctz::civil_day YYYYMMDDToDay(unsigned value)
{
    return cctz::civil_day(
        value / 10000,         // year
        (value % 10000) / 100, // month
        value % 100);          // day
}

cctz::civil_second YYYYMMDDHMMSSToSecond(std::uint64_t value)
{
    return cctz::civil_second(
            value / 10000000000,
            value / 100000000 % 100,
            value / 1000000 % 100,
            value / 10000 % 100,
            value / 100 % 100,
            value % 100);
}


std::vector<const char*> allTimezones()
{
    std::vector<const char*> result;

    auto timezone_name = auto_time_zones;
    while (*timezone_name)
    {
        result.push_back(*timezone_name);
        ++timezone_name;
    }

    return result;
}

struct FailuresCount
{
    size_t non_fatal = 0;
    size_t fatal = 0;
    size_t total = 0;
};

FailuresCount countFailures(const ::testing::TestResult & test_result)
{
    FailuresCount failures{0, 0, 0};
    const size_t count = test_result.total_part_count();
    for (size_t i = 0; i < count; ++i)
    {
        const auto & part = test_result.GetTestPartResult(i);
        if (part.nonfatally_failed())
        {
            ++failures.non_fatal;
            ++failures.total;
        }
        if (part.fatally_failed())
        {
            ++failures.fatal;
            ++failures.total;
        }
    }

    return failures;
}

}

TEST(YYYYMMDDToDay, Test)
{
    std::cerr << YYYYMMDDHMMSSToSecond(19700101'00'00'00) << std::endl;
}

TEST(DateLUTTest, TimeValuesInMiddleOfRange)
{
    const DateLUTImpl lut("Europe/Minsk");
    const time_t time = 1568650811; // 2019-09-16 19:20:11 (Monday)

    EXPECT_EQ(lut.getTimeZone(), "Europe/Minsk");
    EXPECT_EQ(lut.getOffsetAtStartOfEpoch(), 3600*3); // UTC-3

    EXPECT_EQ(lut.toDate(time), 1568581200);
    EXPECT_EQ(lut.toMonth(time), 9);
    EXPECT_EQ(lut.toQuarter(time), 3);
    EXPECT_EQ(lut.toYear(time), 2019);
    EXPECT_EQ(lut.toDayOfMonth(time), 16);

    EXPECT_EQ(lut.toFirstDayOfWeek(time), 1568581200 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayNumOfWeek(time), DayNum(18155) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfMonth(time), 1567285200 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayNumOfMonth(time), DayNum(18140) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayNumOfQuarter(time), DayNum(18078) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfQuarter(time), 1561928400 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayOfYear(time), 1546290000 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayNumOfYear(time), DayNum(17897) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfNextMonth(time), 1569877200 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayOfPrevMonth(time), 1564606800 /*time_t*/);
    EXPECT_EQ(lut.daysInMonth(time), 30 /*UInt8*/);
    EXPECT_EQ(lut.toDateAndShift(time, 10), 1569445200 /*time_t*/);
    EXPECT_EQ(lut.toTime(time), 58811 /*time_t*/);
    EXPECT_EQ(lut.toHour(time), 19 /*unsigned*/);
    EXPECT_EQ(lut.toSecond(time), 11 /*unsigned*/);
    EXPECT_EQ(lut.toMinute(time), 20 /*unsigned*/);
    EXPECT_EQ(lut.toStartOfMinute(time), 1568650800 /*time_t*/);
    EXPECT_EQ(lut.toStartOfFiveMinute(time), 1568650800 /*time_t*/);
    EXPECT_EQ(lut.toStartOfFifteenMinutes(time), 1568650500 /*time_t*/);
    EXPECT_EQ(lut.toStartOfTenMinutes(time), 1568650800 /*time_t*/);
    EXPECT_EQ(lut.toStartOfHour(time), 1568649600 /*time_t*/);
    EXPECT_EQ(lut.toDayNum(time), DayNum(18155) /*DayNum*/);
    EXPECT_EQ(lut.toDayOfYear(time), 259 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeWeekNum(time), 2594 /*unsigned*/);
    EXPECT_EQ(lut.toISOYear(time), 2019 /*unsigned*/);
    EXPECT_EQ(lut.toFirstDayNumOfISOYear(time), DayNum(17896) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfISOYear(time), 1546203600 /*time_t*/);
    EXPECT_EQ(lut.toISOWeek(time), 38 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeMonthNum(time), 24237 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeQuarterNum(time), 8078 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeHourNum(time), 435736 /*time_t*/);
    EXPECT_EQ(lut.toRelativeMinuteNum(time), 26144180 /*time_t*/);
    EXPECT_EQ(lut.toStartOfHourInterval(time, 5), 1568646000 /*time_t*/);
    EXPECT_EQ(lut.toStartOfMinuteInterval(time, 6), 1568650680 /*time_t*/);
    EXPECT_EQ(lut.toStartOfSecondInterval(time, 7), 1568650811 /*time_t*/);
    EXPECT_EQ(lut.toNumYYYYMM(time), 201909 /*UInt32*/);
    EXPECT_EQ(lut.toNumYYYYMMDD(time), 20190916 /*UInt32*/);
    EXPECT_EQ(lut.toNumYYYYMMDDhhmmss(time), 20190916192011 /*UInt64*/);
    EXPECT_EQ(lut.addDays(time, 100), 1577290811 /*time_t*/);
    EXPECT_EQ(lut.addWeeks(time, 100), 1629130811 /*time_t*/);
    EXPECT_EQ(lut.addMonths(time, 100), 1831652411 /*time_t*/);
    EXPECT_EQ(lut.addQuarters(time, 100), 2357655611 /*time_t*/);
    EXPECT_EQ(lut.addYears(time, 10), 1884270011 /*time_t*/);
    EXPECT_EQ(lut.timeToString(time), "2019-09-16 19:20:11" /*std::string*/);
    EXPECT_EQ(lut.dateToString(time), "2019-09-16" /*std::string*/);
}


TEST(DateLUTTest, TimeValuesAtLeftBoderOfRange)
{
    const DateLUTImpl lut("UTC");
    const time_t time = 0; // 1970-01-01 00:00:00 (Thursday)

    EXPECT_EQ(lut.getTimeZone(), "UTC");

    EXPECT_EQ(lut.toDate(time), 0);
    EXPECT_EQ(lut.toMonth(time), 1);
    EXPECT_EQ(lut.toQuarter(time), 1);
    EXPECT_EQ(lut.toYear(time), 1970);
    EXPECT_EQ(lut.toDayOfMonth(time), 1);

    EXPECT_EQ(lut.toFirstDayOfWeek(time), -259200 /*time_t*/); // 1969-12-29 00:00:00
    EXPECT_EQ(lut.toFirstDayNumOfWeek(time), ExtendedDayNum(-3) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfMonth(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayNumOfMonth(time), DayNum(0) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayNumOfQuarter(time), DayNum(0) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfQuarter(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayOfYear(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayNumOfYear(time), DayNum(0) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfNextMonth(time), 2678400 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayOfPrevMonth(time), -2678400 /*time_t*/); // 1969-12-01 00:00:00
    EXPECT_EQ(lut.daysInMonth(time), 31 /*UInt8*/);
    EXPECT_EQ(lut.toDateAndShift(time, 10), 864000 /*time_t*/);
    EXPECT_EQ(lut.toTime(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toHour(time), 0 /*unsigned*/);
    EXPECT_EQ(lut.toSecond(time), 0 /*unsigned*/);
    EXPECT_EQ(lut.toMinute(time), 0 /*unsigned*/);
    EXPECT_EQ(lut.toStartOfMinute(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toStartOfFiveMinute(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toStartOfFifteenMinutes(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toStartOfTenMinutes(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toStartOfHour(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toDayNum(time), DayNum(0) /*DayNum*/);
    EXPECT_EQ(lut.toDayOfYear(time), 1 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeWeekNum(time), 0 /*unsigned*/);
    EXPECT_EQ(lut.toISOYear(time), 1970 /*unsigned*/);
    EXPECT_EQ(lut.toFirstDayNumOfISOYear(time), ExtendedDayNum(-3) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfISOYear(time), -259200 /*time_t*/); // 1969-12-29 00:00:00
    EXPECT_EQ(lut.toISOWeek(time), 1 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeMonthNum(time), 23641 /*unsigned*/); // ?
    EXPECT_EQ(lut.toRelativeQuarterNum(time), 7880 /*unsigned*/); // ?
    EXPECT_EQ(lut.toRelativeHourNum(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toRelativeMinuteNum(time), 0 /*time_t*/);
    EXPECT_EQ(lut.toStartOfHourInterval(time, 5), 0 /*time_t*/);
    EXPECT_EQ(lut.toStartOfMinuteInterval(time, 6), 0 /*time_t*/);
    EXPECT_EQ(lut.toStartOfSecondInterval(time, 7), 0 /*time_t*/);
    EXPECT_EQ(lut.toNumYYYYMM(time), 197001 /*UInt32*/);
    EXPECT_EQ(lut.toNumYYYYMMDD(time), 19700101 /*UInt32*/);
    EXPECT_EQ(lut.toNumYYYYMMDDhhmmss(time), 19700101000000 /*UInt64*/);
    EXPECT_EQ(lut.addDays(time, 100), 8640000 /*time_t*/);
    EXPECT_EQ(lut.addWeeks(time, 100), 60480000 /*time_t*/);
    EXPECT_EQ(lut.addMonths(time, 100), 262828800 /*time_t*/);
    EXPECT_EQ(lut.addQuarters(time, 100), 788918400 /*time_t*/);
    EXPECT_EQ(lut.addYears(time, 10), 315532800 /*time_t*/);
    EXPECT_EQ(lut.timeToString(time), "1970-01-01 00:00:00" /*std::string*/);
    EXPECT_EQ(lut.dateToString(time), "1970-01-01" /*std::string*/);
}

TEST(DateLUTTest, TimeValuesAtRightBoderOfRangeOfOLDLut)
{
    // Value is at the right border of the OLD (small) LUT, and provides meaningful values where OLD LUT would provide garbage.
    const DateLUTImpl lut("UTC");

    const time_t time = 4294343873; // 2106-01-31T01:17:53 (Sunday)

    EXPECT_EQ(lut.getTimeZone(), "UTC");

    EXPECT_EQ(lut.toDate(time), 4294339200);
    EXPECT_EQ(lut.toMonth(time), 1);
    EXPECT_EQ(lut.toQuarter(time), 1);
    EXPECT_EQ(lut.toYear(time), 2106);
    EXPECT_EQ(lut.toDayOfMonth(time), 31);

    EXPECT_EQ(lut.toFirstDayOfWeek(time), 4293820800 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayNumOfWeek(time), DayNum(49697));
    EXPECT_EQ(lut.toFirstDayOfMonth(time), 4291747200 /*time_t*/); // 2016-01-01
    EXPECT_EQ(lut.toFirstDayNumOfMonth(time), DayNum(49673));
    EXPECT_EQ(lut.toFirstDayNumOfQuarter(time), DayNum(49673) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfQuarter(time), 4291747200 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayOfYear(time), 4291747200 /*time_t*/);
    EXPECT_EQ(lut.toFirstDayNumOfYear(time), DayNum(49673) /*DayNum*/);
    EXPECT_EQ(lut.toFirstDayOfNextMonth(time), 4294425600 /*time_t*/); // 2106-02-01
    EXPECT_EQ(lut.toFirstDayOfPrevMonth(time), 4289068800 /*time_t*/); // 2105-12-01
    EXPECT_EQ(lut.daysInMonth(time), 31 /*UInt8*/);
    EXPECT_EQ(lut.toDateAndShift(time, 10), 4295203200 /*time_t*/); // 2106-02-10
    EXPECT_EQ(lut.toTime(time), 4673 /*time_t*/);
    EXPECT_EQ(lut.toHour(time), 1 /*unsigned*/);
    EXPECT_EQ(lut.toMinute(time), 17 /*unsigned*/);
    EXPECT_EQ(lut.toSecond(time), 53 /*unsigned*/);
    EXPECT_EQ(lut.toStartOfMinute(time), 4294343820 /*time_t*/);
    EXPECT_EQ(lut.toStartOfFiveMinute(time), 4294343700 /*time_t*/);
    EXPECT_EQ(lut.toStartOfFifteenMinutes(time), 4294343700 /*time_t*/);
    EXPECT_EQ(lut.toStartOfTenMinutes(time), 4294343400 /*time_t*/);
    EXPECT_EQ(lut.toStartOfHour(time), 4294342800 /*time_t*/);
    EXPECT_EQ(lut.toDayNum(time), DayNum(49703) /*DayNum*/);
    EXPECT_EQ(lut.toDayOfYear(time), 31 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeWeekNum(time), 7100 /*unsigned*/);
    EXPECT_EQ(lut.toISOYear(time), 2106 /*unsigned*/);
    EXPECT_EQ(lut.toFirstDayNumOfISOYear(time), DayNum(49676) /*DayNum*/); // 2106-01-04
    EXPECT_EQ(lut.toFirstDayOfISOYear(time), 4292006400 /*time_t*/);
    EXPECT_EQ(lut.toISOWeek(time), 4 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeMonthNum(time), 25273 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeQuarterNum(time), 8424 /*unsigned*/);
    EXPECT_EQ(lut.toRelativeHourNum(time), 1192873 /*time_t*/);
    EXPECT_EQ(lut.toRelativeMinuteNum(time), 71572397 /*time_t*/);
    EXPECT_EQ(lut.toStartOfHourInterval(time, 5), 4294332000 /*time_t*/);
    EXPECT_EQ(lut.toStartOfMinuteInterval(time, 6), 4294343520 /*time_t*/);
    EXPECT_EQ(lut.toStartOfSecondInterval(time, 7), 4294343872 /*time_t*/);
    EXPECT_EQ(lut.toNumYYYYMM(time), 210601 /*UInt32*/);
    EXPECT_EQ(lut.toNumYYYYMMDD(time), 21060131 /*UInt32*/);
    EXPECT_EQ(lut.toNumYYYYMMDDhhmmss(time), 21060131011753 /*UInt64*/);
    EXPECT_EQ(lut.addDays(time, 100), 4302983873 /*time_t*/);
    EXPECT_EQ(lut.addWeeks(time, 10), 4300391873 /*time_t*/);
    EXPECT_EQ(lut.addMonths(time, 10), 4320523073 /*time_t*/);                 // 2106-11-30 01:17:53
    EXPECT_EQ(lut.addQuarters(time, 10), 4373140673 /*time_t*/);               // 2108-07-31 01:17:53
    EXPECT_EQ(lut.addYears(time, 10), 4609876673 /*time_t*/);                  // 2116-01-31 01:17:53

    EXPECT_EQ(lut.timeToString(time), "2106-01-31 01:17:53" /*std::string*/);
    EXPECT_EQ(lut.dateToString(time), "2106-01-31" /*std::string*/);
}


class DateLUT_TimeZone : public ::testing::TestWithParam<const char * /* timezone name */>
{};

TEST_P(DateLUT_TimeZone, DISABLED_LoadAllTimeZones)
{
    // There are some assumptions and assertions about TZ data made in DateLUTImpl which are verified upon loading,
    // to make sure that those assertions are true for all timezones we are going to load all of them one by one.
    DateLUTImpl{GetParam()};
}

// Another long running test, shouldn't be run to often
TEST_P(DateLUT_TimeZone, VaidateTimeComponentsAroundEpoch)
{
    // Converting time around 1970-01-01 to hour-minute-seconds time components
    // could be problematic.
    const size_t max_failures_per_tz = 3;
    const auto timezone_name = GetParam();

    const auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    const auto lut = DateLUTImpl(timezone_name);

    for (time_t i = -856147870; i < 86400 * 10000; i += 11 * 13 * 17 * 19)
    {
        SCOPED_TRACE(::testing::Message()
                << "\n\tTimezone: " << timezone_name
                << "\n\ttimestamp: " << i
                << "\n\t offset at start of epoch                  : " << lut.getOffsetAtStartOfEpoch()
                << "\n\t offset_is_whole_number_of_hours_everytime : " << lut.getOffsetIsWholNumberOfHoursEveryWhere()
                << "\n\t time_offset_epoch                         : " << lut.getTimeOffsetEpoch()
                << "\n\t offset_at_start_of_lut                    : " << lut.getTimeOffsetAtStartOfLUT());

        EXPECT_GE(24, lut.toHour(i));
        EXPECT_GT(60, lut.toMinute(i));
        EXPECT_GT(60, lut.toSecond(i));

        const auto current_failures = countFailures(*test_info->result());
        if (current_failures.total > 0)
        {
            if (i < 0)
                i = -1;
        }

        if (current_failures.total >= max_failures_per_tz)
            break;
    }
}

TEST_P(DateLUT_TimeZone, getTimeZone)
{
    const auto & lut = DateLUT::instance(GetParam());

    EXPECT_EQ(GetParam(), lut.getTimeZone());
}

TEST_P(DateLUT_TimeZone, ZeroTime)
{
    const auto & lut = DateLUT::instance(GetParam());

    EXPECT_EQ(0, lut.toDayNum(time_t{0}));
    EXPECT_EQ(0, lut.toDayNum(DayNum{0}));
    EXPECT_EQ(0, lut.toDayNum(ExtendedDayNum{0}));
}

// Group of tests for timezones that have or had some time ago an offset which is not multiple of 15 minutes.
INSTANTIATE_TEST_SUITE_P(ExoticTimezones,
    DateLUT_TimeZone,
    ::testing::ValuesIn(std::initializer_list<const char*>{
            "Africa/El_Aaiun",
            "Pacific/Apia",
            "Pacific/Enderbury",
            "Pacific/Fakaofo",
            "Pacific/Kiritimati",
    })
);

INSTANTIATE_TEST_SUITE_P(DISABLED_AllTimeZones,
    DateLUT_TimeZone,
    ::testing::ValuesIn(allTimezones())
);

std::ostream & operator<<(std::ostream & ostr, const DateLUTImpl::Values & v)
{
    return ostr << "DateLUTImpl::Values{"
            << "\n\t date              : " << v.date
            << "\n\t year              : " << static_cast<unsigned int>(v.year)
            << "\n\t month             : " << static_cast<unsigned int>(v.month)
            << "\n\t day               : " << static_cast<unsigned int>(v.day_of_month)
            << "\n\t weekday           : " << static_cast<unsigned int>(v.day_of_week)
            << "\n\t days in month     : " << static_cast<unsigned int>(v.days_in_month)
            << "\n\t offset change     : " << v.amount_of_offset_change()
            << "\n\t offfset change at : " << v.time_at_offset_change()
            << "\n}";
}

struct TimeRangeParam
{
    const cctz::civil_second begin;
    const cctz::civil_second end;
    const int step_in_seconds;
};

std::ostream & operator<<(std::ostream & ostr, const TimeRangeParam & param)
{
    const auto approximate_step = [](const int step) -> std::string
    {
        // Convert seconds to a string of seconds or fractional count of minutes/hours/days.
        static const size_t multipliers[] = {1 /*seconds to seconds*/, 60 /*seconds to minutes*/, 60 /*minutes to hours*/, 24 /*hours to days*/, 0 /*terminator*/};
        static const char* names[] = {"s", "m", "h", "d", nullptr};
        double result = step;
        size_t i = 0;
        for (; i < sizeof(multipliers)/sizeof(multipliers[0]) && result > multipliers[i]; ++i)
            result /= multipliers[i];

        char buffer[256] = {'\0'};
        std::snprintf(buffer, sizeof(buffer), "%.1f%s", result, names[i - 1]);
        return std::string{buffer};
    };

    return ostr << param.begin << " : " << param.end << " step: " << param.step_in_seconds << "s (" << approximate_step(param.step_in_seconds) << ")";
}

class DateLUT_Timezone_TimeRange : public ::testing::TestWithParam<std::tuple<const char* /*timezone_name*/, TimeRangeParam>>
{};

// refactored test from tests/date_lut3.cpp
TEST_P(DateLUT_Timezone_TimeRange, InRange)
{
    // for a time_t values in range [begin, end) to match with reference obtained from cctz:
    // compare date and time components: year, month, day, hours, minutes, seconds, formatted time string.
    const auto & [timezone_name, range_data] = GetParam();
    const auto & [begin, end, step] = range_data;

    const auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    static const size_t max_failures_per_case = 3;
    cctz::time_zone tz;
    ASSERT_TRUE(cctz::load_time_zone(timezone_name, &tz));

    const auto & lut = DateLUT::instance(timezone_name);
    const auto start = cctz::convert(begin, tz).time_since_epoch().count();
    const auto stop = cctz::convert(end, tz).time_since_epoch().count();

    for (time_t expected_time_t = start; expected_time_t < stop; expected_time_t += step)
    {
        SCOPED_TRACE(expected_time_t);

        const auto tz_time = cctz::convert(std::chrono::system_clock::from_time_t(expected_time_t), tz);

        EXPECT_EQ(tz_time.year(), lut.toYear(expected_time_t));
        EXPECT_EQ(tz_time.month(), lut.toMonth(expected_time_t));
        EXPECT_EQ(tz_time.day(), lut.toDayOfMonth(expected_time_t));
        EXPECT_EQ(static_cast<int>(cctz::get_weekday(tz_time)) + 1, lut.toDayOfWeek(expected_time_t)); // tm.tm_wday Sunday is 0, while for DateLUTImpl it is 7
        EXPECT_EQ(cctz::get_yearday(tz_time), lut.toDayOfYear(expected_time_t));
        EXPECT_EQ(tz_time.hour(), lut.toHour(expected_time_t));
        EXPECT_EQ(tz_time.minute(), lut.toMinute(expected_time_t));
        EXPECT_EQ(tz_time.second(), lut.toSecond(expected_time_t));

        const auto time_string = cctz::format("%E4Y-%m-%d %H:%M:%S", std::chrono::system_clock::from_time_t(expected_time_t), tz);
        EXPECT_EQ(time_string, lut.timeToString(expected_time_t));

        // it makes sense to let test execute all checks above to simplify debugging,
        // but once we've found a bad apple, no need to dig deeper.
        if (countFailures(*test_info->result()).total >= max_failures_per_case)
            break;
    }
}

/** Next tests are disabled due to following reasons:
 *  1. They are huge and take enormous amount of time to run
 *  2. Current implementation of DateLUTImpl is inprecise and some cases fail and it seems impractical to try to fix those.
 *  3. Many failures (~300) were fixed while refactoring, about ~40 remain the same and 3 new introduced:
 *      "Asia/Gaza"
 *      "Pacific/Enderbury"
 *      "Pacific/Kiritimati"
 *  So it would be tricky to skip knonw failures to allow all unit tests to pass.
 */
INSTANTIATE_TEST_SUITE_P(DISABLED_AllTimezones_Year2010,
    DateLUT_Timezone_TimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(20101031), YYYYMMDDToDay(20101101), 15 * 60},
            {YYYYMMDDToDay(20100328), YYYYMMDDToDay(20100330), 15 * 60}
        }))
);

INSTANTIATE_TEST_SUITE_P(DISABLED_AllTimezones_Year1970_WHOLE,
    DateLUT_Timezone_TimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(19700101), YYYYMMDDToDay(19701231), 3191 /*53m 11s*/},
        }))
);

INSTANTIATE_TEST_SUITE_P(DISABLED_AllTimezones_Year2010_WHOLE,
    DateLUT_Timezone_TimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(20100101), YYYYMMDDToDay(20101231), 3191 /*53m 11s*/},
        }))
);

INSTANTIATE_TEST_SUITE_P(DISABLED_AllTimezones_Year2020_WHOLE,
    DateLUT_Timezone_TimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(20200101), YYYYMMDDToDay(20201231), 3191 /*53m 11s*/},
        }))
);

INSTANTIATE_TEST_SUITE_P(DISABLED_AllTimezones_PreEpoch,
    DateLUT_Timezone_TimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            {YYYYMMDDToDay(19500101), YYYYMMDDToDay(19600101), 15 * 60},
            {YYYYMMDDToDay(19300101), YYYYMMDDToDay(19350101), 11 * 15 * 60}
        }))
);

INSTANTIATE_TEST_SUITE_P(DISABLED_AllTimezones_Year1970,
    DateLUT_Timezone_TimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            {YYYYMMDDToDay(19700101), YYYYMMDDToDay(19700201), 15 * 60},
            {YYYYMMDDToDay(19700101), YYYYMMDDToDay(19701231), 11 * 13 * 17}
//            // 11 was chosen as a number which can't divide product of 2-combinarions of (7, 24, 60),
//            // to reduce likelehood of hitting same hour/minute/second values for different days.
//            // + 12 is just to make sure that last day is covered fully.
//            {0, 0 + 11 * 3600 * 24 + 12, 11},
        }))
);

