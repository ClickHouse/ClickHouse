#pragma GCC diagnostic ignored "-Wmissing-declarations"
#include <gtest/gtest.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

#include <Common/PODArray.h>

/** Test formatting and parsing predefined DateTime64 values to/from string
 */

using namespace DB;

struct DateTime64StringsTestParam
{
    const std::string_view comment;
    const std::string_view string;
    DateTime64 dt64;
    UInt32 scale;
    const DateLUTImpl & timezone;
};

static std::ostream & operator << (std::ostream & ostr, const DateTime64StringsTestParam & param)
{
    return ostr << param.comment;
}

class DateTime64StringsTest : public ::testing::TestWithParam<DateTime64StringsTestParam> {};
class DateTime64StringParseTest : public DateTime64StringsTest{};
class DateTime64StringParseBestEffortTest : public DateTime64StringsTest{};
class DateTime64StringWriteTest : public DateTime64StringsTest {};


TEST_P(DateTime64StringParseTest, readDateTime64Text)
{
    const auto & param = GetParam();
    ReadBufferFromMemory read_buffer(param.string.data(), param.string.size());

    DateTime64 actual{};
    EXPECT_TRUE(tryReadDateTime64Text(actual, param.scale, read_buffer, param.timezone));

    EXPECT_EQ(param.dt64, actual);
}

TEST_P(DateTime64StringParseTest, parseDateTime64BestEffort)
{
    const auto & param = GetParam();
    ReadBufferFromMemory read_buffer(param.string.data(), param.string.size());

    DateTime64 actual;
    EXPECT_TRUE(tryParseDateTime64BestEffort(actual, param.scale, read_buffer, param.timezone, DateLUT::instance("UTC")));

    EXPECT_EQ(param.dt64, actual);
}

TEST_P(DateTime64StringWriteTest, WriteText)
{
    const auto & param = GetParam();

    PaddedPODArray<char> actual_string(param.string.size() * 2, '\0'); // TODO: detect overflows

    WriteBuffer write_buffer(actual_string.data(), actual_string.size());
    EXPECT_NO_THROW(writeDateTimeText(param.dt64, param.scale, write_buffer, param.timezone));

    EXPECT_STREQ(param.string.data(), actual_string.data());
}

TEST_P(DateTime64StringParseBestEffortTest, parse)
{
    const auto & param = GetParam();
    ReadBufferFromMemory read_buffer(param.string.data(), param.string.size());

    DateTime64 actual;
    EXPECT_TRUE(tryParseDateTime64BestEffort(actual, param.scale, read_buffer, param.timezone, DateLUT::instance("UTC")));

    EXPECT_EQ(param.dt64, actual);
}


// YYYY-MM-DD HH:MM:SS.NNNNNNNNN
INSTANTIATE_TEST_SUITE_P(Basic,
    DateTime64StringParseTest,
    ::testing::ValuesIn(std::initializer_list<DateTime64StringsTestParam>{
        {
            "When subsecond part is missing from string it is set to zero.",
            "2019-09-16 19:20:17",
            1568650817'000,
            3,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When subsecond part is present in string, but it is zero, it is set to zero.",
            "2019-09-16 19:20:17.0",
            1568650817'000,
            3,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When scale is 0, subsecond part is not set.",
            "2019-09-16 19:20:17",
            1568650817ULL,
            0,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When scale is 0, subsecond part is 0 despite being present in string.",
            "2019-09-16 19:20:17.123",
            1568650817ULL,
            0,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When subsecond part is present in string, it is set correctly to DateTime64 value of scale 3.",
            "2019-09-16 19:20:17.123",
            1568650817'123,
            3,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When subsecond part is present in string (and begins with 0), it is set correctly to DateTime64 value of scale 3.",
            "2019-09-16 19:20:17.012",
            1568650817'012,
            3,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When subsecond part scale is smaller than DateTime64 scale, subsecond part is properly adjusted (as if padded from right with zeroes).",
            "2019-09-16 19:20:17.123",
            1568650817'12300ULL,
            5,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When subsecond part scale is larger than DateTime64 scale, subsecond part is truncated.",
            "2019-09-16 19:20:17.123",
            1568650817'1ULL,
            1,
            DateLUT::instance("Europe/Minsk")
        }
    })
);

INSTANTIATE_TEST_SUITE_P(BestEffort,
    DateTime64StringParseBestEffortTest,
    ::testing::ValuesIn(std::initializer_list<DateTime64StringsTestParam>{
        {
            "When subsecond part is unreasonably large, it truncated to given scale",
            "2019-09-16 19:20:17.12345678910111213141516171819202122233435363738393031323334353637383940414243444546474849505152535455565758596061626364",
            1568650817'123456ULL,
            6,
            DateLUT::instance("Europe/Minsk")
        }
    })
);


// TODO: add negative test cases for invalid strings, verifying that error is reported properly

INSTANTIATE_TEST_SUITE_P(Basic,
    DateTime64StringWriteTest,
    ::testing::ValuesIn(std::initializer_list<DateTime64StringsTestParam>{
        {
            "non-zero subsecond part on DateTime64 with scale of 3",
            "2019-09-16 19:20:17.123",
            1568650817'123,
            3,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "non-zero subsecond part on DateTime64 with scale of 5",
            "2019-09-16 19:20:17.12345",
            1568650817'12345ULL,
            5,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "Zero subsecond part is written to string",
            "2019-09-16 19:20:17.000",
            1568650817'000ULL,
            3,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "When scale is 0, subsecond part (and separtor) is missing from string",
            "2019-09-16 19:20:17",
            1568650817ULL,
            0,
            DateLUT::instance("Europe/Minsk")
        },
        {
            "Subsecond part with leading zeroes is written to string correctly",
            "2019-09-16 19:20:17.001",
            1568650817'001ULL,
            3,
            DateLUT::instance("Europe/Minsk")
        }
    })
);

