#include <gtest/gtest.h>

#include <limits>

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <base/scope_guard.h>
#include <Poco/JSON/Parser.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>

using namespace Paimon;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

/// Expected values are the output of the JDK 21 `Double.toString`, `Float.toString` and of Paimon 1.3.2
/// `InternalRowPartitionComputer`.

TEST(PaimonPartitionName, JavaDouble)
{
    const std::pair<double, const char *> cases[] = {
        {2.0, "2.0"}, {1e10, "1.0E10"}, {1e-4, "1.0E-4"}, {0.001, "0.001"}, {9999999.0, "9999999.0"}, {1e7, "1.0E7"},
        {6.25, "6.25"}, {0.1, "0.1"}, {0.0, "0.0"}, {-0.0, "-0.0"}, {100.0, "100.0"}, {123456789.0, "1.23456789E8"},
        {0.00012345, "1.2345E-4"}, {9.9e-4, "9.9E-4"}, {0.3, "0.3"}, {1.0 / 3, "0.3333333333333333"},
        {std::numeric_limits<double>::denorm_min(), "4.9E-324"}, {2 * std::numeric_limits<double>::denorm_min(), "9.9E-324"},
        {3 * std::numeric_limits<double>::denorm_min(), "1.5E-323"}, {2e23, "2.0E23"}, {1e23, "1.0E23"},
        {std::numeric_limits<double>::max(), "1.7976931348623157E308"}, {562949953421312.25, "5.629499534213122E14"},
        {562949953421312.75, "5.629499534213128E14"}, {1000000000000000.25, "1.0000000000000002E15"},
        {std::numeric_limits<double>::quiet_NaN(), "NaN"}, {std::numeric_limits<double>::infinity(), "Infinity"},
        {-std::numeric_limits<double>::infinity(), "-Infinity"}};
    for (const auto & [value, expected] : cases)
        EXPECT_EQ(formatJavaDouble(value), expected);
}

TEST(PaimonPartitionName, JavaFloat)
{
    const std::pair<float, const char *> cases[] = {
        {2.0f, "2.0"}, {1e10f, "1.0E10"}, {1e-4f, "1.0E-4"}, {5.5f, "5.5"}, {0.1f, "0.1"}, {0.3f, "0.3"}, {-2.5f, "-2.5"},
        {std::numeric_limits<float>::denorm_min(), "1.4E-45"}, {2 * std::numeric_limits<float>::denorm_min(), "2.8E-45"},
        {3 * std::numeric_limits<float>::denorm_min(), "4.2E-45"}, {1e7f, "1.0E7"}, {16777216.0f, "1.6777216E7"},
        {std::numeric_limits<float>::max(), "3.4028235E38"}, {std::numeric_limits<float>::min(), "1.1754944E-38"},
        {4194303.75f, "4194303.8"}, {8388607.5f, "8388607.5"}};
    for (const auto & [value, expected] : cases)
        EXPECT_EQ(formatJavaFloat(value), expected);
}

TEST(PaimonPartitionName, NullOrWhitespaceOnly)
{
    /// U+1680, U+2000, U+2006, U+2008, U+200A, U+2028, U+2029, U+205F, U+3000 are whitespace in Java.
    for (const char * value :
         {"", " ", "\t\n\v\f\r", "\x1C\x1D\x1E\x1F", "\xE1\x9A\x80", "\xE2\x80\x80", "\xE2\x80\x86", "\xE2\x80\x88", "\xE2\x80\x8A",
          "\xE2\x80\xA8", "\xE2\x80\xA9", "\xE2\x81\x9F", "\xE3\x80\x80", " \xE3\x80\x80\t"})
        EXPECT_TRUE(isNullOrWhitespaceOnly(value)) << value;
    /// U+00A0, U+2007, U+202F, U+180E, U+200B, U+FEFF, U+0085 are not, nor is invalid UTF-8.
    for (const char * value :
         {"a", " a ", "\xC2\xA0", "\xE2\x80\x87", "\xE2\x80\xAF", "\xE1\xA0\x8E", "\xE2\x80\x8B", "\xEF\xBB\xBF", "\xC2\x85", "\xFF",
          " \xE3\x80"})
        EXPECT_FALSE(isNullOrWhitespaceOnly(value)) << value;
    EXPECT_FALSE(isNullOrWhitespaceOnly(std::string_view("\0", 1)));
}

TEST(PaimonPartitionName, DateTimeCastToString)
{
    EXPECT_EQ(formatDateCastToString(19723), "2024-01-01");
    EXPECT_EQ(formatDateCastToString(2932896), "9999-12-31");
    EXPECT_EQ(formatDateCastToString(-719162), "0001-01-01");
    EXPECT_EQ(formatDateCastToString(-1), "1969-12-31");
    EXPECT_EQ(formatDateCastToString(0), "1970-01-01");

    EXPECT_EQ(formatTimeCastToString(37800000, 0), "10:30:00");
    EXPECT_EQ(formatTimeCastToString(37800120, 3), "10:30:00.12");
    EXPECT_EQ(formatTimeCastToString(37800000, 3), "10:30:00.0");
    EXPECT_EQ(formatTimeCastToString(37800123, 1), "10:30:00.1");
    EXPECT_EQ(formatTimeCastToString(-1, 3), "23:59:59.999");

    const DateLUTImpl & utc = DateLUT::instance("UTC");
    const Timestamp whole{.millisecond = 1705318200000, .nano_of_millisecond = 0};
    const Timestamp fraction{.millisecond = 1705318200123, .nano_of_millisecond = 456000};
    EXPECT_EQ(formatTimestampCastToString(whole, 6, utc), "2024-01-15 11:30:00.000000");
    EXPECT_EQ(formatTimestampCastToString(fraction, 6, utc), "2024-01-15 11:30:00.123456");
    EXPECT_EQ(formatTimestampCastToString(whole, 3, utc), "2024-01-15 11:30:00.000");
    EXPECT_EQ(formatTimestampCastToString(fraction, 3, utc), "2024-01-15 11:30:00.123456");
    EXPECT_EQ(formatTimestampCastToString(whole, 0, utc), "2024-01-15 11:30:00");
    EXPECT_EQ(formatTimestampCastToString(fraction, 9, utc), "2024-01-15 11:30:00.123456000");
    EXPECT_EQ(formatTimestampCastToString(Timestamp{.millisecond = -1}, 6, utc), "1969-12-31 23:59:59.999000");
    EXPECT_EQ(formatTimestampCastToString(whole, 6, DateLUT::instance("Asia/Shanghai")), "2024-01-15 19:30:00.000000");
}

/// The option is read from the schema, and a local time zone timestamp is rendered in the server time zone.
TEST(PaimonPartitionName, BucketPathOfLocalTimeZoneTimestamp)
{
    const String previous_time_zone = DateLUT::serverTimezoneInstance().getTimeZone();
    DateLUT::setDefaultTimezone("Asia/Shanghai");
    SCOPE_EXIT({ DateLUT::setDefaultTimezone(previous_time_zone); });

    /// A one-field BinaryRow: big-endian arity, the null bitset, then the 8-byte little-endian slot holding
    /// the millisecond of a compact timestamp, 2024-01-15 11:30:00 UTC.
    String row("\0\0\0\x01", 4);
    row.append(8, '\0');
    for (size_t i = 0; i < 8; ++i)
        row.push_back(static_cast<char>((1705318200000ULL >> (i * 8)) & 0xFF));

    auto bucket_path = [&](const String & options)
    {
        const String json = R"({"version": 3, "id": 0, "highestFieldId": 0, "timeMillis": 0, "partitionKeys": ["p"],
            "primaryKeys": [], "fields": [{"id": 0, "name": "p", "type": "TIMESTAMP(3) WITH LOCAL TIME ZONE"}], "options": {)"
            + options + "}}";
        const PaimonTableSchema schema(Poco::JSON::Parser().parse(json).extract<Poco::JSON::Object::Ptr>());
        return getBucketPath(row, 0, schema, "__DEFAULT_PARTITION__");
    };

    EXPECT_EQ(bucket_path(""), "p=2024-01-15T11%3A30/bucket-0");
    EXPECT_EQ(bucket_path(R"("partition.legacy-name": "FALSE")"), "p=2024-01-15 19%3A30%3A00.000/bucket-0");
    try
    {
        bucket_path(R"("partition.legacy-name": "no")");
        FAIL() << "expected an exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::BAD_ARGUMENTS);
    }
}
