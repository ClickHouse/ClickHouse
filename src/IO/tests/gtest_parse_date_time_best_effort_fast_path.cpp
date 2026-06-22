#include <gtest/gtest.h>

#include <string_view>

#include <IO/ConcatReadBuffer.h>
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

struct TryResult
{
    bool ok;
    time_t res;
    bool operator==(const TryResult &) const = default;
};

TryResult tryParseBestEffort(std::string_view s, const DateLUTImpl & tz)
{
    ReadBufferFromMemory in(s.data(), s.size());
    time_t res = 0;
    bool ok = tryParseDateTimeBestEffort(res, in, tz, DateLUT::instance("UTC"));
    return {ok, res};
}

/// Parse `s` from a ReadBuffer that is split into two chunks at byte offset `split`, so that the
/// chunk boundary (a real `buffer().end()`) falls inside the value. This mimics a streaming ReadBuffer
/// where a value can be delivered across two refills - the case the canonical fast path must handle.
TryResult tryParseBestEffortSplit(std::string_view s, size_t split, const DateLUTImpl & tz)
{
    const std::string_view head = s.substr(0, split);
    const std::string_view tail = s.substr(split);
    ReadBufferFromMemory part1(head.data(), head.size());
    ReadBufferFromMemory part2(tail.data(), tail.size());
    ConcatReadBuffer in(part1, part2);
    /// Prime the buffer so the first chunk becomes the working buffer (its end is the split point).
    /// Without this the buffer is empty on entry and the fast path skips it - it would not exercise
    /// the boundary at all. A real streaming buffer is likewise already filled when parsing starts.
    char probe = 0;
    in.peek(probe);
    time_t res = 0;
    bool ok = tryParseDateTimeBestEffort(res, in, tz, DateLUT::instance("UTC"));
    return {ok, res};
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

/// An over-long numeric field (a trailing digit after the day or second) must not be mistaken for the
/// canonical layout. The general parser rejects it, so the fast path must too.
TEST(ParseDateTimeBestEffortFastPath, OverlongFieldsRejected)
{
    const auto & utc = DateLUT::instance("UTC");

    /// Trailing digit after the 10-byte date and after the 19-byte date-time: both are malformed.
    EXPECT_FALSE(tryParseBestEffort("2019-08-201", utc).ok);
    EXPECT_FALSE(tryParseBestEffort("2019-08-20 10:18:561", utc).ok);
}

/// Regression test for the ReadBuffer chunk-boundary bug: when a value is split exactly after the
/// canonical prefix, the fast path must not treat the buffer end as a clean token end. For example
/// "2019-08-201" split after "2019-08-20" must still be rejected (an over-long day field), not parsed
/// as "2019-08-20" + a stray "1". Streaming and single-buffer parsing must agree at every split point.
TEST(ParseDateTimeBestEffortFastPath, ChunkBoundarySplitMatchesSingleBuffer)
{
    const auto & utc = DateLUT::instance("UTC");

    const std::string_view inputs[] = {
        "2019-08-201",            /// malformed: trailing digit after the date -> must be rejected
        "2019-08-20 10:18:561",   /// malformed: trailing digit after the date-time -> must be rejected
        "2019-08-20",             /// valid date only
        "2019-08-20 10:18:56",    /// valid date-time
        "2019-08-20T10:18:56",    /// valid date-time, 'T' separator
        "2019-08-20 10:18:56+01:00", /// valid date-time with timezone offset
    };

    for (const auto & s : inputs)
    {
        const TryResult whole = tryParseBestEffort(s, utc);
        for (size_t split = 1; split < s.size(); ++split)
            EXPECT_EQ(tryParseBestEffortSplit(s, split, utc), whole)
                << "input: " << s << " split at: " << split;
    }
}
