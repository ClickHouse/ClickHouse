#include <gtest/gtest.h>

#include <string>
#include <vector>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

using namespace DB;


TEST(WriteHelpersTest, ToStringWithFinalSeparatorTest)
{
    {
        std::vector<std::string> v;
        EXPECT_EQ(toStringWithFinalSeparator(v, " or "), "");
    }
    {
        std::vector<std::string> v = {"AAA"};
        EXPECT_EQ(toStringWithFinalSeparator(v, " or "), "'AAA'");
    }
    {
        std::vector<std::string> v = {"AAA", "BBB"};
        EXPECT_EQ(toStringWithFinalSeparator(v, " or "), "'AAA' or 'BBB'");
    }
    {
        std::vector<std::string> v = {"AAA", "BBB", "CCC"};
        EXPECT_EQ(toStringWithFinalSeparator(v, " or "), "'AAA', 'BBB' or 'CCC'");
    }
    {
        std::vector<std::string> v = {"AAA", "BBB", "CCC", "DDD"};
        EXPECT_EQ(toStringWithFinalSeparator(v, " or "), "'AAA', 'BBB', 'CCC' or 'DDD'");
    }
}


namespace DB::ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
}

template <typename T>
static void expectDecimalTextThrows(UInt32 scale, bool fixed_fractional_length = false, UInt32 fractional_length = 0)
{
    WriteBufferFromOwnString buf;
    try
    {
        writeText(Decimal<T>(1), scale, buf, false, fixed_fractional_length, fractional_length);
        FAIL() << "expected ARGUMENT_OUT_OF_BOUND for scale " << scale << ", fractional length " << fractional_length;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }
}

/// A `DecimalField` does not validate its scale, and `FieldVisitorToString` formats it with `writeText`.
/// A scale of 2^31 or more used to reach a division by zero in `DecimalUtils::getWholePart`.
TEST(WriteHelpersTest, DecimalTextOutOfRangeScaleThrows)
{
    for (UInt32 scale : {77u, 0x80000000u, 0xffffffffu})
    {
        expectDecimalTextThrows<Int32>(scale);
        expectDecimalTextThrows<Int64>(scale);
        expectDecimalTextThrows<Int128>(scale);
        expectDecimalTextThrows<Int256>(scale);
    }
    expectDecimalTextThrows<Int32>(2, true, 77);
    expectDecimalTextThrows<Int32>(0x80000000u, true, 2);

    WriteBufferFromOwnString buf;
    writeText(Decimal32(12345), 2, buf);
    EXPECT_EQ(buf.str(), "123.45");
}
