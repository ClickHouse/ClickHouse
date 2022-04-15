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
