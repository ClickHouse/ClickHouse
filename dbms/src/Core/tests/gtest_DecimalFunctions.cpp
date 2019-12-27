#pragma GCC diagnostic ignored "-Wmissing-declarations"
#include <gtest/gtest.h>

#include <Core/DecimalFunctions.h>

namespace
{
using namespace DB;

struct DecimalUtilsSplitAndCombineTestParam
{
    const char * description;

    Decimal64 decimal_value;
    UInt8 scale;

    DecimalUtils::DecimalComponents<typename Decimal64::NativeType> components;
};

std::ostream & operator << (std::ostream & ostr, const DecimalUtilsSplitAndCombineTestParam & param)
{
    return ostr << param.description;
}

class DecimalUtilsSplitAndCombineTest : public ::testing::TestWithParam<DecimalUtilsSplitAndCombineTestParam>
{};

template <typename DecimalType>
void testSplit(const DecimalUtilsSplitAndCombineTestParam & param)
{
    const DecimalType decimal_value = param.decimal_value;
    const auto & actual_components = DecimalUtils::split(decimal_value, param.scale);

    EXPECT_EQ(param.components.whole, actual_components.whole);
    EXPECT_EQ(param.components.fractional, actual_components.fractional);
}

template <typename DecimalType>
void testDecimalFromComponents(const DecimalUtilsSplitAndCombineTestParam & param)
{
    EXPECT_EQ(param.decimal_value,
              DecimalUtils::decimalFromComponents<DecimalType>(param.components.whole, param.components.fractional, param.scale));
}

template <typename DecimalType>
void testGetWhole(const DecimalUtilsSplitAndCombineTestParam & param)
{
    EXPECT_EQ(param.components.whole,
              DecimalUtils::getWholePart(DecimalType{param.decimal_value}, param.scale));
}

template <typename DecimalType>
void testGetFractional(const DecimalUtilsSplitAndCombineTestParam & param)
{
    EXPECT_EQ(param.components.fractional,
              DecimalUtils::getFractionalPart(DecimalType{param.decimal_value}, param.scale));
}

// unfortunatelly typed parametrized tests () are not supported in this version of gtest, so I have to emulate by hand.
TEST_P(DecimalUtilsSplitAndCombineTest, split_Decimal32)
{
    testSplit<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, split_Decimal64)
{
    testSplit<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, split_Decimal128)
{
    testSplit<Decimal128>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, combine_Decimal32)
{
    testDecimalFromComponents<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, combine_Decimal64)
{
    testDecimalFromComponents<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, combine_Decimal128)
{
    testDecimalFromComponents<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getWholePart_Decimal32)
{
    testGetWhole<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getWholePart_Decimal64)
{
    testGetWhole<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getWholePart_Decimal128)
{
    testGetWhole<Decimal128>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getFractionalPart_Decimal32)
{
    testGetFractional<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getFractionalPart_Decimal64)
{
    testGetFractional<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getFractionalPart_Decimal128)
{
    testGetFractional<Decimal128>(GetParam());
}

}

// Intentionally small values that fit into 32-bit in order to cover Decimal32, Decimal64 and Decimal128 with single set of data.
INSTANTIATE_TEST_CASE_P(Basic,
    DecimalUtilsSplitAndCombineTest,
    ::testing::ValuesIn(std::initializer_list<DecimalUtilsSplitAndCombineTestParam>{
        {
            "Positive value with non-zero scale, whole, and fractional parts.",
            1234567'89,
            2,
            {
                1234567,
                89
            }
        },
        {
            "When scale is 0, fractional part is 0.",
            1234567'89,
            0,
            {
                123456789,
                0
            }
        },
        {
            "When scale is not 0 and fractional part is 0.",
            1234567'00,
            2,
            {
                1234567,
                0
            }
        },
        {
            "When scale is not 0 and whole part is 0.",
            123,
            3,
            {
                0,
                123
            }
        },
        {
            "For negative Decimal value whole part is negative, fractional is non-negative.",
            -1234567'89,
            2,
            {
                -1234567,
                89
            }
        }
    }
),);
