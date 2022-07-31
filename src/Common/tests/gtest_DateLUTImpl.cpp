#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include <cctz/time_zone.h>


/// For the expansion of gtest macros.
#if defined(__clang__)
    #pragma clang diagnostic ignored "-Wused-but-marked-unused"
#endif

// All timezones present at build time and embedded into ClickHouse binary.
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

std::vector<const char*> allTimezones(bool with_weird_offsets = true)
{
    std::vector<const char*> result;

    const auto * timezone_name = auto_time_zones;
    while (*timezone_name)
    {
        bool weird_offsets = (std::string_view(*timezone_name) == "Africa/Monrovia");

        if (!weird_offsets || with_weird_offsets)
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

TEST(DateLUTTest, makeDayNumTest)
{
    const DateLUTImpl & lut = DateLUT::instance("UTC");
    EXPECT_EQ(0, lut.makeDayNum(1924, 12, 31));
    EXPECT_EQ(-1, lut.makeDayNum(1924, 12, 31, -1));
    EXPECT_EQ(-16436, lut.makeDayNum(1925, 1, 1));
    EXPECT_EQ(0, lut.makeDayNum(1970, 1, 1));
    EXPECT_EQ(114635, lut.makeDayNum(2283, 11, 11));
    EXPECT_EQ(114635, lut.makeDayNum(2500, 12, 25));
}


TEST(DateLUTTest, TimeValuesInMiddleOfRange)
{
    const DateLUTImpl & lut = DateLUT::instance("Europe/Minsk");
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
    const DateLUTImpl & lut = DateLUT::instance("UTC");
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

TEST(DateLUTTest, TimeValuesAtRightBoderOfRangeOfOldLUT)
{
    // Value is at the right border of the old (small) LUT, and provides meaningful values where old LUT would provide garbage.
    const DateLUTImpl & lut = DateLUT::instance("UTC");

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


class DateLUTWithTimeZone : public ::testing::TestWithParam<const char * /* timezone name */>
{};

TEST_P(DateLUTWithTimeZone, LoadLUT)
{
    // There are some assumptions and assertions about TZ data made in DateLUTImpl which are verified upon loading,
    // to make sure that those assertions are true for all timezones we are going to load all of them one by one.
    DateLUT::instance(GetParam());
}

// Another long running test, shouldn't be run to often
TEST_P(DateLUTWithTimeZone, VaidateTimeComponentsAroundEpoch)
{
    // Converting time around 1970-01-01 to hour-minute-seconds time components
    // could be problematic.
    const size_t max_failures_per_tz = 3;
    const auto * timezone_name = GetParam();

    const auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    const DateLUTImpl & lut = DateLUT::instance(timezone_name);

    for (time_t i = -856147870; i < 86400 * 10000; i += 11 * 13 * 17 * 19)
    {
        SCOPED_TRACE(::testing::Message()
                << "\n\tTimezone: " << timezone_name
                << "\n\ttimestamp: " << i
                << "\n\t offset at start of epoch                  : " << lut.getOffsetAtStartOfEpoch()
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

TEST_P(DateLUTWithTimeZone, getTimeZone)
{
    const auto & lut = DateLUT::instance(GetParam());

    EXPECT_EQ(GetParam(), lut.getTimeZone());
}


// Group of tests for timezones that have or had some time ago an offset which is not multiple of 15 minutes.
INSTANTIATE_TEST_SUITE_P(ExoticTimezones,
    DateLUTWithTimeZone,
    ::testing::ValuesIn(std::initializer_list<const char*>{
            "Africa/El_Aaiun",
            "Pacific/Apia",
            "Pacific/Enderbury",
            "Pacific/Fakaofo",
            "Pacific/Kiritimati",
    })
);

INSTANTIATE_TEST_SUITE_P(AllTimeZones,
    DateLUTWithTimeZone,
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
            << "\n\t offset change at : " << v.time_at_offset_change()
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
    return ostr << param.begin << " : " << param.end << " step: " << param.step_in_seconds << "s";
}

class DateLUTWithTimeZoneAndTimeRange : public ::testing::TestWithParam<std::tuple<const char* /*timezone_name*/, TimeRangeParam>>
{};

// refactored test from tests/date_lut3.cpp
TEST_P(DateLUTWithTimeZoneAndTimeRange, InRange)
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

        const cctz::civil_second tz_time = cctz::convert(std::chrono::system_clock::from_time_t(expected_time_t), tz);

        /// Weird offset, not supported.
        /// Example: Africa/Monrovia has offset UTC-0:44:30 in year 1970.

        auto timestamp_current_day_pre = std::chrono::system_clock::to_time_t(tz.lookup(cctz::civil_day(tz_time)).pre);
        auto timestamp_current_day_post = std::chrono::system_clock::to_time_t(tz.lookup(cctz::civil_day(tz_time) + 1).post);

        if (timestamp_current_day_pre % 900 || timestamp_current_day_post % 900)
            continue;

        /// Unsupported timezone transitions - not in 15-minute time point or to different day.
        /// Example: America/Goose_Bay decided to go back one hour at 00:01:
        /// $ seq 1289097900 30 1289103600 | TZ=America/Goose_Bay LC_ALL=C xargs -I{} date -d @{}
        /// Sat Nov  6 23:59:00 ADT 2010
        /// Sat Nov  6 23:59:30 ADT 2010
        /// Sun Nov  7 00:00:00 ADT 2010
        /// Sun Nov  7 00:00:30 ADT 2010
        /// Sat Nov  6 23:01:00 AST 2010
        /// Sat Nov  6 23:01:30 AST 2010

        bool has_transition = false;
        cctz::time_zone::civil_transition transition{};
        if (tz.next_transition(std::chrono::system_clock::from_time_t(expected_time_t - 1), &transition)
            && (transition.from.day() == tz_time.day() || transition.to.day() == tz_time.day()))
        {
            has_transition = true;
        }

        if (has_transition && (transition.from.second() != 0 || transition.from.minute() % 15 != 0))
        {
            /*std::cerr << "Skipping " << timezone_name << " " << tz_time
                << " because of unsupported timezone transition from " << transition.from << " to " << transition.to
                << " (not divisible by 15 minutes)\n";*/
            continue;
        }

        /// Transition to previous day, but not from midnight.
        if (has_transition && cctz::civil_day(transition.from) == cctz::civil_day(transition.to) + 1
            && transition.from != cctz::civil_day(transition.from))
        {
            /*std::cerr << "Skipping " << timezone_name << " " << tz_time
                << " because of unsupported timezone transition from " << transition.from << " to " << transition.to
                << " (to previous day but not at midnight)\n";*/
            continue;
        }

        /// To large transition.
        if (has_transition
            && std::abs(transition.from - transition.to) > 3600 * 3)
        {
            /*std::cerr << "Skipping " << timezone_name << " " << tz_time
                << " because of unsupported timezone transition from " << transition.from << " to " << transition.to
                << " (it is too large)\n";*/
            continue;
        }

        EXPECT_EQ(tz_time.year(), lut.toYear(expected_time_t));
        EXPECT_EQ(tz_time.month(), lut.toMonth(expected_time_t));
        EXPECT_EQ(tz_time.day(), lut.toDayOfMonth(expected_time_t));
        /// tm.tm_wday Sunday is 0, while for DateLUTImpl it is 7
        EXPECT_EQ(static_cast<int>(cctz::get_weekday(tz_time)) + 1, lut.toDayOfWeek(expected_time_t));
        EXPECT_EQ(cctz::get_yearday(tz_time), lut.toDayOfYear(expected_time_t));
        EXPECT_EQ(tz_time.hour(), lut.toHour(expected_time_t));
        EXPECT_EQ(tz_time.minute(), lut.toMinute(expected_time_t));
        EXPECT_EQ(tz_time.second(), lut.toSecond(expected_time_t));

        const auto time_string = cctz::format("%E4Y-%m-%d %H:%M:%S", std::chrono::system_clock::from_time_t(expected_time_t), tz);
        EXPECT_EQ(time_string, lut.timeToString(expected_time_t));

        /// It makes sense to let test execute all checks above to simplify debugging,
        /// but once we've found a bad apple, no need to dig deeper.
        if (countFailures(*test_info->result()).total >= max_failures_per_case)
            break;
    }
}

INSTANTIATE_TEST_SUITE_P(AllTimezones_Year2010,
    DateLUTWithTimeZoneAndTimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(20101031), YYYYMMDDToDay(20101101), 10 * 15 * 60},
            {YYYYMMDDToDay(20100328), YYYYMMDDToDay(20100330), 10 * 15 * 60}
        }))
);

INSTANTIATE_TEST_SUITE_P(AllTimezones_Year1970_WHOLE,
    DateLUTWithTimeZoneAndTimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones(false)),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(19700101), YYYYMMDDToDay(19701231), 10 * 3191 /*53m 11s*/},
        }))
);

INSTANTIATE_TEST_SUITE_P(AllTimezones_Year2010_WHOLE,
    DateLUTWithTimeZoneAndTimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones(false)),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(20100101), YYYYMMDDToDay(20101231), 10 * 3191 /*53m 11s*/},
        }))
);

INSTANTIATE_TEST_SUITE_P(AllTimezones_Year2020_WHOLE,
    DateLUTWithTimeZoneAndTimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones()),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            // Values from tests/date_lut3.cpp
            {YYYYMMDDToDay(20200101), YYYYMMDDToDay(20201231), 10 * 3191 /*53m 11s*/},
        }))
);

INSTANTIATE_TEST_SUITE_P(AllTimezones_PreEpoch,
    DateLUTWithTimeZoneAndTimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones(false)),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            {YYYYMMDDToDay(19500101), YYYYMMDDToDay(19600101), 10 * 15 * 60},
            {YYYYMMDDToDay(19300101), YYYYMMDDToDay(19350101), 10 * 11 * 15 * 60}
        }))
);

INSTANTIATE_TEST_SUITE_P(AllTimezones_Year1970,
    DateLUTWithTimeZoneAndTimeRange,
    ::testing::Combine(
        ::testing::ValuesIn(allTimezones(false)),
        ::testing::ValuesIn(std::initializer_list<TimeRangeParam>{
            {YYYYMMDDToDay(19700101), YYYYMMDDToDay(19700201), 10 * 15 * 60},
            {YYYYMMDDToDay(19700101), YYYYMMDDToDay(19701231), 10 * 11 * 13 * 17}
//            // 11 was chosen as a number which can't divide product of 2-combinarions of (7, 24, 60),
//            // to reduce likelehood of hitting same hour/minute/second values for different days.
//            // + 12 is just to make sure that last day is covered fully.
//            {0, 0 + 11 * 3600 * 24 + 12, 11},
        }))
);

