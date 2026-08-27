#include <gtest/gtest.h>

#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <limits>

using namespace DB;

namespace
{
PrometheusQueryParsingUtil::RequestTimestampType parseRequestTimestamp(std::string_view input)
{
    PrometheusQueryParsingUtil::RequestTimestampType result{0};
    String error_message;
    size_t error_pos = 0;
    EXPECT_TRUE(PrometheusQueryParsingUtil::tryParsePrometheusRequestTimestamp(
        input, 9, result, &error_message, &error_pos))
        << input << ": " << error_message << " at position " << error_pos;
    return result;
}
}

TEST(PromQLParser, PrometheusRequestTimestampAcceptsStrictHttpForms)
{
    EXPECT_EQ(parseRequestTimestamp("1000").value, static_cast<Int128>(1000'000'000'000LL));
    EXPECT_EQ(parseRequestTimestamp("1970-01-01T00:16:40Z").value, static_cast<Int128>(1000'000'000'000LL));
    EXPECT_EQ(parseRequestTimestamp("1970-01-01T01:16:40.123456789+01:00").value, static_cast<Int128>(1000'123'456'789LL));
    EXPECT_EQ(parseRequestTimestamp("1970-01-01T00:16:40,123Z").value, static_cast<Int128>(1000'123'000'000LL));
}

TEST(PromQLParser, PrometheusRequestTimestampAcceptsGoFloatSyntax)
{
    EXPECT_EQ(parseRequestTimestamp("1_000").value, static_cast<Int128>(1000'000'000'000LL));
    EXPECT_EQ(parseRequestTimestamp("0x1p4").value, static_cast<Int128>(16'000'000'000LL));
}

TEST(PromQLParser, PrometheusRequestTimestampRoundsNumericValuesToMilliseconds)
{
    EXPECT_EQ(parseRequestTimestamp("1000.9994").value, static_cast<Int128>(1000'999'000'000LL));
    EXPECT_EQ(parseRequestTimestamp("1000.9996").value, static_cast<Int128>(1001'000'000'000LL));
    EXPECT_EQ(parseRequestTimestamp("-0.9996").value, static_cast<Int128>(-1'000'000'000LL));
}

TEST(PromQLParser, PrometheusRequestTimestampMatchesFloat64RoundingAtHalfMillisecondBoundaries)
{
    constexpr Int128 base = static_cast<Int128>(1'700'000'000'000'000'000LL);
    EXPECT_EQ(parseRequestTimestamp("1700000000.0004999").value, base);
    EXPECT_EQ(parseRequestTimestamp("1700000000.0005000").value, base);
    EXPECT_EQ(parseRequestTimestamp("1700000000.0005001").value, base + 1'000'000);

    EXPECT_EQ(parseRequestTimestamp("-1700000000.0004999").value, -base);
    EXPECT_EQ(parseRequestTimestamp("-1700000000.0005000").value, -base);
    EXPECT_EQ(parseRequestTimestamp("-1700000000.0005001").value, -base - 1'000'000);
}

TEST(PromQLParser, PrometheusRequestTimestampAcceptsPrometheusTimeBounds)
{
    const Int128 min_time_seconds = static_cast<Int128>(std::numeric_limits<Int64>::min()) / 1000 + 62135596801;
    const Int128 max_time_seconds = static_cast<Int128>(std::numeric_limits<Int64>::max()) / 1000 - 62135596801;
    constexpr Int128 scale = 1'000'000'000;

    EXPECT_EQ(
        parseRequestTimestamp("-292273086-05-16T16:47:06Z").value,
        min_time_seconds * scale);
    EXPECT_EQ(
        parseRequestTimestamp("292277025-08-18T07:12:54.999999999Z").value,
        max_time_seconds * scale + 999'999'999);
}

TEST(PromQLParser, PrometheusRequestTimestampValidatesRFC3339CalendarDates)
{
    for (const auto *const input : {
             "2023-02-29T00:00:00Z",
             "2024-02-30T00:00:00Z",
             "2024-04-31T00:00:00Z",
             "2024-06-31T00:00:00Z",
             "2024-09-31T00:00:00Z",
             "2024-11-31T00:00:00Z",
             "2024-00-01T00:00:00Z",
             "2024-13-01T00:00:00Z",
             "2024-01-00T00:00:00Z",
             "0000-02-30T00:00:00Z",
         })
    {
        PrometheusQueryParsingUtil::RequestTimestampType result;
        EXPECT_FALSE(PrometheusQueryParsingUtil::tryParsePrometheusRequestTimestamp(input, 9, result)) << input;
    }

    for (const auto *const input : {
             "2023-02-28T00:00:00Z",
             "2024-02-29T00:00:00Z",
             "2024-04-30T00:00:00Z",
             "0000-02-29T00:00:00Z",
             "9999-12-31T23:59:59Z",
         })
    {
        PrometheusQueryParsingUtil::RequestTimestampType result;
        String error_message;
        size_t error_pos = 0;
        EXPECT_TRUE(PrometheusQueryParsingUtil::tryParsePrometheusRequestTimestamp(
            input, 9, result, &error_message, &error_pos))
            << input << ": " << error_message << " at position " << error_pos;
    }
}

TEST(PromQLParser, PrometheusRequestTimestampAcceptsFloatUnderflowAsZero)
{
    EXPECT_EQ(parseRequestTimestamp("1e-4000").value, 0);
    EXPECT_EQ(parseRequestTimestamp("-1e-4000").value, 0);
}

TEST(PromQLParser, PrometheusRequestTimestampRejectsNonHttpForms)
{
    for (const auto *const input : {
             "5m",
             "0x10",
             "1970-01-01",
             "1970-01-01 00:16:40",
             "1970-01-01T00:16:40",
             "1970-01-01T00:16:40+0100",
         })
    {
        PrometheusQueryParsingUtil::RequestTimestampType result;
        EXPECT_FALSE(PrometheusQueryParsingUtil::tryParsePrometheusRequestTimestamp(input, 9, result));
    }
}
