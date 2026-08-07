#include <gtest/gtest.h>
#include <Core/AccurateComparison.h>

using namespace DB;


GTEST_TEST(AccurateComparison, Tests)
{
    /// Arbitrary assortion of cases.

    ASSERT_TRUE(accurate::equalsOp(static_cast<Float32>(123), static_cast<UInt64>(123)));
    ASSERT_TRUE(accurate::lessOp(static_cast<Float32>(123), static_cast<UInt64>(124)));
    ASSERT_TRUE(accurate::lessOp(static_cast<Float32>(-1), static_cast<UInt64>(1)));
    ASSERT_TRUE(accurate::lessOp(static_cast<Int64>(-1), static_cast<UInt64>(1)));
    ASSERT_TRUE(!accurate::equalsOp(static_cast<Int64>(-1), static_cast<UInt64>(-1)));

    ASSERT_TRUE(accurate::equalsOp(-0., 0));
    ASSERT_TRUE(accurate::lessOp(-0., 1));
    ASSERT_TRUE(accurate::lessOp(-0.5, 1));
    ASSERT_TRUE(accurate::lessOp(0.5, 1));
    ASSERT_TRUE(accurate::equalsOp(1.0, 1));
    ASSERT_TRUE(accurate::greaterOp(1.1, 1));
    ASSERT_TRUE(accurate::greaterOp(11.1, 1));
    ASSERT_TRUE(accurate::greaterOp(11.1, 11));
    ASSERT_TRUE(accurate::lessOp(-11.1, 11));
    ASSERT_TRUE(accurate::lessOp(-11.1, -11));
    ASSERT_TRUE(accurate::lessOp(-1.1, -1));
    ASSERT_TRUE(accurate::greaterOp(-1.1, -2));
    ASSERT_TRUE(accurate::greaterOp(1000., 100));
    ASSERT_TRUE(accurate::greaterOp(-100., -1000));
    ASSERT_TRUE(accurate::lessOp(100., 1000));
    ASSERT_TRUE(accurate::lessOp(-1000., -100));

    ASSERT_TRUE(accurate::lessOp(-std::numeric_limits<Float64>::infinity(), 0));
    ASSERT_TRUE(accurate::lessOp(-std::numeric_limits<Float64>::infinity(), 1000));
    ASSERT_TRUE(accurate::lessOp(-std::numeric_limits<Float64>::infinity(), -1000));
    ASSERT_TRUE(accurate::greaterOp(std::numeric_limits<Float64>::infinity(), 0));
    ASSERT_TRUE(accurate::greaterOp(std::numeric_limits<Float64>::infinity(), 1000));
    ASSERT_TRUE(accurate::greaterOp(std::numeric_limits<Float64>::infinity(), -1000));

    ASSERT_TRUE(accurate::lessOp(1, 1e100));
    ASSERT_TRUE(accurate::lessOp(-1, 1e100));
    ASSERT_TRUE(accurate::lessOp(-1e100, 1));
    ASSERT_TRUE(accurate::lessOp(-1e100, -1));

    /// Tricky cases with floats.

    ASSERT_TRUE(accurate::equalsOp(static_cast<UInt64>(9223372036854775808ULL), static_cast<Float64>(9223372036854775808ULL)));
    ASSERT_TRUE(accurate::equalsOp(static_cast<UInt64>(9223372036854775808ULL), static_cast<Float32>(9223372036854775808ULL)));

    ASSERT_TRUE(accurate::greaterOp(static_cast<UInt64>(9223372036854776000ULL), static_cast<Float64>(9223372036854776000ULL)));
    ASSERT_TRUE(accurate::lessOp(static_cast<UInt64>(9223372000000000000ULL), static_cast<Float32>(9223372000000000000ULL)));

    ASSERT_TRUE(accurate::equalsOp(static_cast<Float32>(9223372036854775808ULL), static_cast<Float64>(9223372036854775808ULL)));

    /// Integers

    ASSERT_TRUE(accurate::lessOp(static_cast<UInt8>(255), 300));
    ASSERT_TRUE(accurate::lessOp(static_cast<UInt8>(255), static_cast<Int16>(300)));
    ASSERT_TRUE(accurate::notEqualsOp(static_cast<UInt8>(255), 44));
    ASSERT_TRUE(accurate::notEqualsOp(static_cast<UInt8>(255), static_cast<Int16>(44)));


/*    Float32 f = static_cast<Float32>(9223372000000000000ULL);
    UInt64 u = static_cast<UInt64>(9223372000000000000ULL);
    DecomposedFloat32 components(f);

    std::cerr << std::fixed << std::setprecision(3) << f
        << ", " << components.normalized_exponent()
        << ", " << components.mantissa()
        << ", " << (components.mantissa() << (components.normalized_exponent() - 23))
        << ", " << (1ULL << components.normalized_exponent())
        << ", " << (components.normalized_exponent() >= static_cast<int16_t>(8 * sizeof(UInt64) - is_signed_v<UInt64>))
        << ": " << components.compare(u)
        << "\n";*/
}
