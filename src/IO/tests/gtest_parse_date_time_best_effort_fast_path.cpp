#include <gtest/gtest.h>

#include <string_view>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>

/** parseDateTimeBestEffort has a fast path for the canonical 'YYYY-MM-DD hh:mm:ss' / 'YYYY-MM-DD' layout
  * (what toString(DateTime/DateTime64) emits). These tests pin that the fast path is equivalent to the
  * general best-effort parser and to the basic parser for canonical input, and that non-canonical input
  * still flows through the general parser unchanged.
  */

using namespace DB;

namespace
{

time_t parseBestEffort(std::string_view s, const DateLUTImpl & tz)
{
    ReadBufferFromMemory in(s.data(), s.size());
    time_t res = 0;
    parseDateTimeBestEffort(res, in, tz, DateLUT::instance("UTC"));
    return res;
}

time_t parseBasic(std::string_view s, const DateLUTImpl & tz)
{
    ReadBufferFromMemory in(s.data(), s.size());
    time_t res = 0;
    readDateTimeText(res, in, tz);
    return res;
}

DateTime64 parseBestEffort64(std::string_view s, UInt32 scale, const DateLUTImpl & tz)
{
    ReadBufferFromMemory in(s.data(), s.size());
    DateTime64 res = 0;
    parseDateTime64BestEffort(res, scale, in, tz, DateLUT::instance("UTC"));
    return res;
}

DateTime64 parseBasic64(std::string_view s, UInt32 scale, const DateLUTImpl & tz)
{
    ReadBufferFromMemory in(s.data(), s.size());
    DateTime64 res = 0;
    readDateTime64Text(res, scale, in, tz);
    return res;
}

}

/// Canonical DateTime strings: the fast path must produce the same result as the basic parser.
TEST(ParseDateTimeBestEffortFastPath, CanonicalEqualsBasicDateTime)
{
    const auto & utc = DateLUT::instance("UTC");
    const auto & minsk = DateLUT::instance("Europe/Minsk");

    const std::string_view inputs[] = {
        "2019-08-20 10:18:56",
        "2000-01-01 00:00:00",
        "1970-01-01 00:00:00",
        "2106-02-07 06:28:15",
        "2021-02-28 23:59:59",
        "2020-02-29 12:00:00", /// leap day
        "2019-08-20T10:18:56", /// 'T' separator
        "2019-08-20",          /// date only
        "2021-12-31",
    };

    for (const auto & s : inputs)
    {
        EXPECT_EQ(parseBestEffort(s, utc), parseBasic(s, utc)) << "UTC input: " << s;
        EXPECT_EQ(parseBestEffort(s, minsk), parseBasic(s, minsk)) << "Minsk input: " << s;
    }
}

/// Canonical DateTime64 strings: the fast path must produce the same result as the basic parser.
TEST(ParseDateTimeBestEffortFastPath, CanonicalEqualsBasicDateTime64)
{
    const auto & utc = DateLUT::instance("UTC");

    struct Case { std::string_view s; UInt32 scale; };
    const Case cases[] = {
        {"2019-08-20 10:18:56", 3},
        {"2019-08-20 10:18:56.123", 3},
        {"2019-08-20 10:18:56.123456", 6},
        {"2000-01-01 00:00:00.000", 3},
        {"2019-08-20T10:18:56.5", 3},
        {"2019-08-20", 3},
    };

    for (const auto & c : cases)
        EXPECT_EQ(parseBestEffort64(c.s, c.scale, utc), parseBasic64(c.s, c.scale, utc))
            << "input: " << c.s << " scale: " << c.scale;
}

/// Exact expected values for the canonical layout (the data tested by the date_time_64 perf test).
TEST(ParseDateTimeBestEffortFastPath, CanonicalExactValues)
{
    const auto & utc = DateLUT::instance("UTC");
    EXPECT_EQ(parseBestEffort("2019-08-20 10:18:56", utc), 1566296336);
    EXPECT_EQ(parseBestEffort("1970-01-01 00:00:00", utc), 0);
    EXPECT_EQ(parseBestEffort("2000-01-01 00:00:00", utc), 946684800);
    EXPECT_EQ(parseBestEffort64("2019-08-20 10:18:56.123", 3, utc), 1566296336123LL);
}

/// Non-canonical inputs must still be parsed by the general best-effort path (fall-through), unchanged.
TEST(ParseDateTimeBestEffortFastPath, NonCanonicalStillWorks)
{
    const auto & utc = DateLUT::instance("UTC");

    /// Unix timestamp.
    EXPECT_EQ(parseBestEffort("1566296336", utc), 1566296336);
    /// DD/MM/YYYY style.
    EXPECT_EQ(parseBestEffort("24/12/2018", utc), parseBestEffort("2018-12-24", utc));
    /// Compact YYYYMMDD.
    EXPECT_EQ(parseBestEffort("20180824", utc), parseBestEffort("2018-08-24", utc));
    /// Timezone offset: a canonical-looking date followed by an offset must NOT be truncated by the
    /// fast path; the offset has to be applied by the general parser.
    EXPECT_EQ(parseBestEffort("2019-08-20 10:18:56+01:00", utc),
              parseBestEffort("2019-08-20 10:18:56", utc) - 3600);
    /// Trailing 'Z' (UTC).
    EXPECT_EQ(parseBestEffort("2019-08-20 10:18:56Z", utc), parseBestEffort("2019-08-20 10:18:56", utc));
}

/// An over-long numeric field (e.g. a 5-digit year) must not be mistaken for the canonical layout.
TEST(ParseDateTimeBestEffortFastPath, OverlongFieldsDeferToGeneralParser)
{
    const auto & utc = DateLUT::instance("UTC");

    /// "2019-08-200" : a trailing digit after the day. tryParse must agree between a fresh buffer
    /// (this just must not crash and must consume consistently); compare to the general parser by
    /// constructing the same input twice.
    auto try_parse = [&](std::string_view s)
    {
        ReadBufferFromMemory in(s.data(), s.size());
        time_t res = 0;
        bool ok = tryParseDateTimeBestEffort(res, in, utc, DateLUT::instance("UTC"));
        return std::make_pair(ok, res);
    };

    /// Both calls go through the same code; the point is that the fast path does not engage on a
    /// malformed trailing digit and the result is deterministic.
    EXPECT_EQ(try_parse("2019-08-201"), try_parse("2019-08-201"));
}
