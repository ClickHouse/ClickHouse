#include <gtest/gtest.h>

#include <string>
#include <vector>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Core/DecimalFunctions.h>
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

template <typename T>
static std::string decimalText(T value, UInt32 scale, bool fixed_fractional_length = false, UInt32 fractional_length = 0)
{
    WriteBufferFromOwnString buf;
    writeText(Decimal<T>(value), scale, buf, false, fixed_fractional_length, fractional_length);
    return buf.str();
}

template <typename T>
static void checkDecimalScaleBoundary()
{
    constexpr UInt32 max_scale = DecimalUtils::max_precision<Decimal<T>>;
    expectDecimalTextThrows<T>(max_scale + 1);
    /// Rounding to fewer fractional digits multiplies by a power of ten that depends on the scale.
    expectDecimalTextThrows<T>(max_scale + 1, true, 0);
    EXPECT_NO_THROW(decimalText<T>(1, max_scale));
    EXPECT_NO_THROW(decimalText<T>(1, max_scale, true, 0));
}

/// A `DecimalField` does not validate its scale, and `FieldVisitorToString` formats it with `writeText`.
/// A scale of 2^31 or more used to reach a division by zero in `DecimalUtils::getWholePart`,
/// and a scale wider than the type overflowed the rounding arithmetic.
TEST(WriteHelpersTest, DecimalTextOutOfRangeScaleThrows)
{
    for (UInt32 scale : {77u, 0x80000000u, 0xffffffffu})
    {
        expectDecimalTextThrows<Int32>(scale);
        expectDecimalTextThrows<Int64>(scale);
        expectDecimalTextThrows<Int128>(scale);
        expectDecimalTextThrows<Int256>(scale);
    }
    expectDecimalTextThrows<Int32>(0x80000000u, true, 2);

    checkDecimalScaleBoundary<Int32>();
    checkDecimalScaleBoundary<Int64>();
    checkDecimalScaleBoundary<Int128>();
    checkDecimalScaleBoundary<Int256>();

    /// 77 fractional digits are supported (`toDecimalString` accepts them), 78 are not.
    expectDecimalTextThrows<Int32>(2, true, 78);
    EXPECT_EQ(decimalText<Int32>(12345, 2, true, 77), "123.45" + std::string(75, '0'));

    EXPECT_EQ(decimalText<Int32>(12345, 2), "123.45");
}
