#if !defined(SANITIZER)

/// This test is slow due to exhaustive checking of time zones.
/// Better to replace with randomization.
/// Also, recommended to replace with a functional test for better maintainability.

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-int-conversion"
#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#pragma clang diagnostic pop

#include <chrono>

/// For the expansion of gtest macros.
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

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
        const auto & part = test_result.GetTestPartResult(static_cast<int>(i));
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

TEST(DateLUTTest, hasFixedOffset)
{
    EXPECT_TRUE(DateLUT::instance("UTC").hasFixedOffset());
    EXPECT_TRUE(DateLUT::instance("Etc/GMT-5").hasFixedOffset());
    EXPECT_TRUE(DateLUT::instance("Etc/GMT+5").hasFixedOffset());
    EXPECT_FALSE(DateLUT::instance("Europe/Berlin").hasFixedOffset());
    EXPECT_FALSE(DateLUT::instance("America/New_York").hasFixedOffset());
    /// No DST nowadays, but the offset changed within the LUT range (e.g. in 1942).
    EXPECT_FALSE(DateLUT::instance("Asia/Kolkata").hasFixedOffset());
}

TEST(DateLUTTest, dayShiftStaysWithinLUT)
{
    const DateLUTImpl & lut = DateLUT::instance("UTC");
    const time_t lut_min = -2208988800; /// 1900-01-01 00:00:00 UTC, the first second of the LUT
    const time_t lut_max = 10413792000 - 1; /// 2299-12-31 23:59:59 UTC, the last second of the LUT

    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(0, 1));
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(0, -1));
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(lut_min, 0));
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(lut_min, 1));
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(lut_max, -1));
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(lut_min, 146096)); /// 1900-01-01 -> 2299-12-31, the last LUT day

    /// Input outside the LUT.
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(lut_min - 1, 0));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(lut_max + 1, 0));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(std::numeric_limits<time_t>::min(), 0));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(std::numeric_limits<time_t>::max(), 0));

    /// Result outside the LUT.
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(lut_min, -1));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(lut_max, 1));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(lut_min, 146097));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(0, std::numeric_limits<Int64>::min()));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(0, std::numeric_limits<Int64>::max()));

    /// Scaled values (raw DateTime64 representation).
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(lut_min * 1000, 1, 1000));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(lut_min * 1000, -1, 1000));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(lut_min * 1000 - 1000, 0, 1000));
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT((lut_max + 1) * 1000 - 1, -1, 1000));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT((lut_max + 1) * 1000, 0, 1000));
    /// Negative sub-second values shifted to the upper LUT edge: the calendar path truncates the
    /// division towards zero, so the fast path must decline to keep the results identical.
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(-1, 120530, 1000));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(-259200001, 17219 * 7, 1000));
    /// For scale 9 the end of the LUT exceeds Int64, so only the lower bound and Int64 overflow apply.
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(0, 1, 1000000000));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(0, -25568, 1000000000));
    EXPECT_TRUE(lut.dayShiftStaysWithinLUT(std::numeric_limits<Int64>::max() - 86400 * 1000000000L, 1, 1000000000));
    EXPECT_FALSE(lut.dayShiftStaysWithinLUT(std::numeric_limits<Int64>::max(), 1, 1000000000));
}

TEST(DateLUTTest, makeDayNumTest)
{
    const DateLUTImpl & lut = DateLUT::instance("UTC");
    EXPECT_EQ(0, lut.makeDayNum(1899, 12, 31));
    EXPECT_EQ(-1, lut.makeDayNum(1899, 12, 31, -1));
    EXPECT_EQ(-25567, lut.makeDayNum(1900, 1, 1));
    EXPECT_EQ(-16436, lut.makeDayNum(1925, 1, 1));
    EXPECT_EQ(0, lut.makeDayNum(1970, 1, 1));
    EXPECT_EQ(120529, lut.makeDayNum(2300, 12, 31));
    EXPECT_EQ(120529, lut.makeDayNum(2500, 12, 25));
}


namespace
{

/// Start-of-day Unix time of a civil day in a time zone, matching DateLUTImpl's tie-break for ambiguous midnights.
Int64 referenceStartOfDay(const cctz::time_zone & tz, const cctz::civil_day & day)
{
    const cctz::time_zone::civil_lookup lookup = tz.lookup(day);
    const auto tp = lookup.trans < lookup.post ? lookup.post : lookup.pre;
    return std::chrono::duration_cast<cctz::seconds>(tp.time_since_epoch()).count();
}

UInt8 referenceDayOfWeek(const cctz::civil_day & day)
{
    switch (cctz::get_weekday(day))
    {
        case cctz::weekday::monday:     return 1;
        case cctz::weekday::tuesday:    return 2;
        case cctz::weekday::wednesday:  return 3;
        case cctz::weekday::thursday:   return 4;
        case cctz::weekday::friday:     return 5;
        case cctz::weekday::saturday:   return 6;
        case cctz::weekday::sunday:     return 7;
    }
    return 0;
}

}

/// DateTime64 now supports values outside of [1900, 2299]. For out-of-range time points, DateLUTImpl
/// falls back to cctz; here we cross-check the fallback against an independent cctz computation, sweeping
/// across the 1900 and 2300 boundaries for several time zones (including ones with DST and non-whole-hour offsets).
TEST(DateLUTTest, TimeValuesOutOfRange)
{
    const std::vector<const char *> zones = {"UTC", "Europe/Moscow", "America/New_York", "Asia/Kolkata", "Australia/Lord_Howe"};

    for (const char * zone : zones)
    {
        const DateLUTImpl & lut = DateLUT::instance(zone);
        cctz::time_zone tz;
        ASSERT_TRUE(cctz::load_time_zone(zone, &tz)) << zone;

        /// Only out-of-range years: the in-range path keeps its own (pre-existing) behaviour for time zones
        /// whose offset has a sub-minute component before 1900 (e.g. Moscow's +2:30:17 LMT).
        for (Int64 year : {1000, 1500, 1700, 1850, 1899, 2300, 2350, 2500, 5000, 9999})
        {
            for (int month : {1, 2, 3, 6, 9, 12})
            {
                const cctz::civil_second cs(year, month, 14, 13, 47, 5);
                const Int64 t = cctz::convert(cs, tz).time_since_epoch().count();

                const cctz::civil_second local = cctz::convert(std::chrono::system_clock::from_time_t(static_cast<time_t>(t)), tz);
                const cctz::civil_day cd(local);

                SCOPED_TRACE(std::string(zone) + " year=" + std::to_string(year) + " month=" + std::to_string(month)
                    + " t=" + std::to_string(t));

                EXPECT_EQ(lut.toYear(t), local.year());
                EXPECT_EQ(lut.toMonth(t), local.month());
                EXPECT_EQ(lut.toDayOfMonth(t), local.day());
                EXPECT_EQ(lut.toDayOfWeek(t), referenceDayOfWeek(cd));
                EXPECT_EQ(lut.toHour(t), static_cast<unsigned>(local.hour()));
                EXPECT_EQ(lut.toMinute(t), static_cast<unsigned>(local.minute()));
                EXPECT_EQ(lut.toSecond(t), static_cast<unsigned>(local.second()));

                EXPECT_EQ(lut.toDate(t), referenceStartOfDay(tz, cd));

                const auto comp = lut.toDateTimeComponents(t);
                EXPECT_EQ(comp.date.year, local.year());
                EXPECT_EQ(comp.date.month, local.month());
                EXPECT_EQ(comp.date.day, local.day());
                EXPECT_EQ(comp.time.hour, static_cast<UInt64>(local.hour()));
                EXPECT_EQ(comp.time.minute, local.minute());
                EXPECT_EQ(comp.time.second, local.second());

                /// makeDateTime is the inverse for unambiguous local times.
                if (tz.lookup(cs).kind == cctz::time_zone::civil_lookup::UNIQUE)
                    EXPECT_EQ(lut.makeDateTime(static_cast<Int16>(local.year()), static_cast<UInt8>(local.month()),
                        static_cast<UInt8>(local.day()), static_cast<UInt8>(local.hour()),
                        static_cast<UInt8>(local.minute()), static_cast<UInt8>(local.second())), t);

                /// Start of month / year.
                EXPECT_EQ(lut.toDayOfYear(t), static_cast<UInt16>(cd - cctz::civil_day(local.year(), 1, 1) + 1));
                EXPECT_EQ(lut.toFirstDayOfMonth(t), referenceStartOfDay(tz, cctz::civil_day(local.year(), local.month(), 1)));
                EXPECT_EQ(lut.toFirstDayOfYear(t), referenceStartOfDay(tz, cctz::civil_day(local.year(), 1, 1)));
            }
        }
    }
}

/// Calendar arithmetic must keep working across the boundaries of the lookup table.
TEST(DateLUTTest, ArithmeticOutOfRange)
{
    const DateLUTImpl & lut = DateLUT::instance("UTC");

    /// 2250-06-15 12:00:00 UTC, in range; adding 100 years lands in 2350 (out of range).
    const Int64 in_range = cctz::convert(cctz::civil_second(2250, 6, 15, 12, 0, 0), cctz::utc_time_zone()).time_since_epoch().count();
    EXPECT_EQ(lut.toYear(lut.addYears(in_range, 100)), 2350);
    EXPECT_EQ(lut.toYear(lut.addYears(in_range, -400)), 1850);
    EXPECT_EQ(lut.toMonth(lut.addMonths(in_range, 1300)), 10);  // +108 years 4 months -> 2358-10
    EXPECT_EQ(lut.toYear(lut.addMonths(in_range, 1300)), 2358);

    /// Out-of-range input.
    const Int64 oor = cctz::convert(cctz::civil_second(1850, 3, 31, 0, 0, 0), cctz::utc_time_zone()).time_since_epoch().count();
    EXPECT_EQ(lut.toYear(lut.addYears(oor, 1)), 1851);
    EXPECT_EQ(lut.toMonth(lut.addMonths(oor, 1)), 4);   // 1850-03-31 + 1 month -> 1850-04-30
    EXPECT_EQ(lut.toDayOfMonth(lut.addMonths(oor, 1)), 30);
    EXPECT_EQ(lut.toYear(lut.addDays(oor, 365)), 1851);

    /// Extreme deltas must not overflow the in-range escape-detection guards; the result saturates to [0000, 9999].
    const Int64 max_delta = std::numeric_limits<Int64>::max();
    const Int64 min_delta = std::numeric_limits<Int64>::min();
    EXPECT_EQ(lut.toYear(lut.addYears(in_range, max_delta)), 9999);
    EXPECT_EQ(lut.toYear(lut.addYears(in_range, min_delta)), 0);
    EXPECT_EQ(lut.toYear(lut.addMonths(in_range, max_delta)), 9999);
    EXPECT_EQ(lut.toYear(lut.addMonths(in_range, min_delta)), 0);
    EXPECT_EQ(lut.toYear(lut.addDays(in_range, max_delta)), 9999);
    EXPECT_EQ(lut.toYear(lut.addDays(in_range, min_delta)), 0);
}

/// An extreme Date32 day number (outside the representable [0000, 9999] window) must saturate so that calendar
/// components stay in range. Regression for a day-of-year that overflowed to 64169 (broke formatDateTime '%j').
TEST(DateLUTTest, DayOfYearExtendedDayNumOutOfRange)
{
    const DateLUTImpl & lut = DateLUT::instance("UTC");

    for (Int32 daynum : {std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max(), -1'000'000'000, 1'000'000'000})
    {
        const ExtendedDayNum d{daynum};
        SCOPED_TRACE("daynum=" + std::to_string(daynum));
        const auto day_of_year = lut.toDayOfYear(d);
        EXPECT_GE(day_of_year, 1);
        EXPECT_LE(day_of_year, 366);
    }

    /// The negative extreme saturates to 0000-01-01, the positive one to 9999-12-31.
    EXPECT_EQ(lut.toDayOfYear(ExtendedDayNum{std::numeric_limits<Int32>::min()}), 1);
    EXPECT_EQ(lut.toYear(ExtendedDayNum{std::numeric_limits<Int32>::min()}), 0);
    EXPECT_EQ(lut.toDayOfYear(ExtendedDayNum{std::numeric_limits<Int32>::max()}), 365);
    EXPECT_EQ(lut.toYear(ExtendedDayNum{std::numeric_limits<Int32>::max()}), 9999);
}

/// Interval rounding must floor towards -inf for pre-1970 day numbers: truncating division rounds towards zero,
/// i.e. forward in time, past the start of the interval. (The result is masked by the narrow Date return type of
/// the SQL `toStartOfInterval`, so verify the DateLUT primitives directly.)
TEST(DateLUTTest, StartOfIntervalPreEpochFloor)
{
    const DateLUTImpl & lut = DateLUT::instance("UTC");

    struct YMD { int y; int m; int d; };
    const YMD dates[] = {{1950, 3, 15}, {1905, 11, 30}, {1820, 7, 4}};

    for (const auto & ymd : dates)
    {
        const ExtendedDayNum dn{static_cast<Int32>(cctz::civil_day{ymd.y, ymd.m, ymd.d} - cctz::civil_day{1970, 1, 1})};
        SCOPED_TRACE(std::to_string(ymd.y) + "-" + std::to_string(ymd.m) + "-" + std::to_string(ymd.d));

        for (UInt64 weeks : {2ul, 3ul, 5ul})
        {
            const auto start = lut.toStartOfWeekInterval(dn, weeks);
            EXPECT_LE(start.toUnderType(), dn.toUnderType());                                          // not in the future
            EXPECT_LT(dn.toUnderType() - start.toUnderType(), static_cast<Int32>(weeks * 7));
            EXPECT_EQ(lut.toDayOfWeek(start), 1);                                                      // weeks start on Monday
        }

        for (UInt64 months : {2ul, 7ul, 25ul})
        {
            const auto start = lut.toStartOfMonthInterval(dn, months);
            EXPECT_LE(start.toUnderType(), dn.toUnderType());
            EXPECT_EQ(lut.toDayOfMonth(start), 1);                                                     // first day of a month
        }
    }
}

/// Week / ISO computations are timezone-independent and repeat every 400 years. Verify the periodicity holds
/// across the boundary (year Y vs Y + 400) for the out-of-range escape path.
TEST(DateLUTTest, WeekFunctionsOutOfRangePeriodicity)
{
    const DateLUTImpl & lut = DateLUT::instance("UTC");

    for (int month : {1, 3, 7, 12})
    {
        const Int64 oor = cctz::convert(cctz::civil_second(1850, month, 14, 0, 0, 0), cctz::utc_time_zone()).time_since_epoch().count();
        const Int64 in_range = cctz::convert(cctz::civil_second(2250, month, 14, 0, 0, 0), cctz::utc_time_zone()).time_since_epoch().count();

        EXPECT_EQ(lut.toISOWeek(oor), lut.toISOWeek(in_range)) << "month=" << month;
        EXPECT_EQ(lut.toYearWeek(oor, 0).second, lut.toYearWeek(in_range, 0).second) << "month=" << month;
        EXPECT_EQ(lut.toISOYear(oor) + 400, lut.toISOYear(in_range)) << "month=" << month;
        EXPECT_EQ(lut.toYearWeek(oor, 0).first + 400, lut.toYearWeek(in_range, 0).first) << "month=" << month;
    }
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
    EXPECT_EQ(lut.toStartOfFiveMinutes(time), 1568650800 /*time_t*/);
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
    EXPECT_EQ(lut.toStableRelativeHourNum(time), 435757 /*time_t*/);
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
    EXPECT_EQ(lut.toLastDayOfWeek(time), 1569099600 /*time_t*/);
    EXPECT_EQ(lut.toLastDayNumOfWeek(time), DayNum(18161) /*DayNum*/);
    EXPECT_EQ(lut.toLastDayOfMonth(time), 1569790800 /*time_t*/);
    EXPECT_EQ(lut.toLastDayNumOfMonth(time), DayNum(18169) /*DayNum*/);
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
    EXPECT_EQ(lut.toStartOfFiveMinutes(time), 0 /*time_t*/);
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
    EXPECT_EQ(lut.toStableRelativeHourNum(time), 24 /*time_t*/);
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
    EXPECT_EQ(lut.toLastDayOfWeek(time), 259200 /*time_t*/);
    EXPECT_EQ(lut.toLastDayNumOfWeek(time), DayNum(3) /*DayNum*/);
    EXPECT_EQ(lut.toLastDayOfMonth(time), 2592000 /*time_t*/);
    EXPECT_EQ(lut.toLastDayNumOfMonth(time), DayNum(30) /*DayNum*/);
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
    EXPECT_EQ(lut.toFirstDayOfMonth(time), 4291747200 /*time_t*/); // 2106-01-01
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
    EXPECT_EQ(lut.toStartOfFiveMinutes(time), 4294343700 /*time_t*/);
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
    EXPECT_EQ(lut.toStableRelativeHourNum(time), 1192897 /*time_t*/);
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
    EXPECT_EQ(lut.toLastDayOfWeek(time), 4294339200 /*time_t*/);
    EXPECT_EQ(lut.toLastDayNumOfWeek(time), DayNum(49703) /*DayNum*/);
    EXPECT_EQ(lut.toLastDayOfMonth(time), 4294339200 /*time_t*/); // 2106-01-01
    EXPECT_EQ(lut.toLastDayNumOfMonth(time), DayNum(49703));
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

TEST_P(DateLUTWithTimeZone, FixedOffsetImpliesArithmeticDayAddition)
{
    /// The day/week addition fast path relies on this property for every time zone that reports a fixed offset.
    const DateLUTImpl & lut = DateLUT::instance(GetParam());
    if (!lut.hasFixedOffset())
        return;

    const size_t max_failures_per_tz = 3;
    const auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();

    /// Keep t + delta * 86400 within the LUT range [1900-01-01, 2299-12-31].
    for (time_t t = -2199999999; t < 10300000000; t += 11 * 13 * 17 * 19 * 23)
    {
        for (Int64 delta : {-100, -7, -1, 0, 1, 7, 100})
            EXPECT_EQ(lut.addDays(t, delta), t + delta * 86400) << "timezone: " << GetParam() << ", timestamp: " << t << ", delta: " << delta;

        if (countFailures(*test_info->result()).total >= max_failures_per_tz)
            break;
    }
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

[[maybe_unused]] static std::ostream & operator<<(std::ostream & ostr, const DateLUTImpl::Values & v)
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

static std::ostream & operator<<(std::ostream & ostr, const TimeRangeParam & param)
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

#endif
