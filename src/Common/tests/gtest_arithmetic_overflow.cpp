#include <gtest/gtest.h>

#include <limits>
#include <type_traits>

#include <base/arithmeticOverflow.h>

namespace arithmetic_overflow_test
{

static volatile bool use_boundary_value = true;

template <typename T>
NO_INLINE T runtimeValue(T value)
{
    /// Keep the boundary opaque even under LTO, so the old UB-based helpers
    /// cannot pass by constant-propagating the value into the wrapper.
    return use_boundary_value ? value : T{};
}

/// Keep each arithmetic relation in one out-of-line function. The old
/// UB-based implementation allowed Clang to fold these relations to false.
template <typename T>
NO_INLINE bool addWraps(T value)
{
    return common::addIgnoreOverflow(value, T{1}) < value;
}

template <typename T>
NO_INLINE bool subWraps(T value)
{
    return common::subIgnoreOverflow(value, T{1}) > value;
}

template <typename T>
NO_INLINE bool mulWraps(T value)
{
    return common::mulIgnoreOverflow(value, T{2}) < T{0};
}

template <typename T>
NO_INLINE bool negateWraps(T value)
{
    return common::negateIgnoreOverflow(value) < T{0};
}

template <typename T>
void checkIgnoreOverflowWraparound()
{
    const T min = runtimeValue(std::numeric_limits<T>::min());
    const T max = runtimeValue(std::numeric_limits<T>::max());

    EXPECT_EQ(common::addIgnoreOverflow(max, T{1}), min);
    EXPECT_EQ(common::subIgnoreOverflow(min, T{1}), max);
    EXPECT_EQ(common::mulIgnoreOverflow(max, T{2}), T{-2});
    EXPECT_EQ(common::negateIgnoreOverflow(min), min);

    EXPECT_TRUE(addWraps(max));
    EXPECT_TRUE(subWraps(min));
    EXPECT_TRUE(mulWraps(max));
    EXPECT_TRUE(negateWraps(min));
}

/// Narrow operands that undergo the usual integral promotions (`Int16`,
/// `UInt8`) have `decltype(x op y) == int`. The helpers must wrap in that
/// promoted result type: switching them to `make_unsigned_t<T1>` would
/// truncate to 8 or 16 bits and silently change the callers' results.
///
/// `Int8` is deliberately not covered here - it is `signed _BitInt(8)`, which
/// does not promote, so it belongs to `checkIgnoreOverflowWraparound` instead.
template <typename T>
void checkPromotedNarrowKeepsResultType()
{
    const T min = runtimeValue(std::numeric_limits<T>::min());
    const T max = runtimeValue(std::numeric_limits<T>::max());

    static_assert(std::is_same_v<decltype(common::addIgnoreOverflow(max, T{1})), int>);
    static_assert(std::is_same_v<decltype(common::subIgnoreOverflow(min, T{1})), int>);
    static_assert(std::is_same_v<decltype(common::mulIgnoreOverflow(max, T{2})), int>);
    static_assert(std::is_same_v<decltype(common::negateIgnoreOverflow(min)), int>);

    constexpr int int_min = static_cast<int>(std::numeric_limits<T>::min());
    constexpr int int_max = static_cast<int>(std::numeric_limits<T>::max());

    /// Nothing wraps around here: the promoted `int` result has room for all
    /// of these, even though none of them fit back into `T`.
    EXPECT_EQ(common::addIgnoreOverflow(max, T{1}), int_max + 1);
    EXPECT_EQ(common::subIgnoreOverflow(min, T{1}), int_min - 1);
    EXPECT_EQ(common::mulIgnoreOverflow(max, T{2}), int_max * 2);
    EXPECT_EQ(common::mulIgnoreOverflow(min, T{2}), int_min * 2);
    EXPECT_EQ(common::negateIgnoreOverflow(min), -int_min);
    EXPECT_EQ(common::negateIgnoreOverflow(max), -int_max);
}

}

GTEST_TEST(ArithmeticOverflow, IgnoreOverflowWraparound)
{
    /// Builtin integers reproduce the optimizer-sensitive failure. Wide
    /// integers guard the same contract for the shared helper templates.
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int8>();
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int32>();
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int64>();
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int128>();
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int256>();
}

GTEST_TEST(ArithmeticOverflow, IgnoreOverflowPromotedNarrowOperands)
{
    arithmetic_overflow_test::checkPromotedNarrowKeepsResultType<Int16>();
    arithmetic_overflow_test::checkPromotedNarrowKeepsResultType<UInt8>();
}
