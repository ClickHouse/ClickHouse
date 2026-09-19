#include <gtest/gtest.h>

#include <limits>

#include <base/arithmeticOverflow.h>

namespace arithmetic_overflow_test
{

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
    const T min = std::numeric_limits<T>::min();
    const T max = std::numeric_limits<T>::max();

    EXPECT_EQ(common::addIgnoreOverflow(max, T{1}), min);
    EXPECT_EQ(common::subIgnoreOverflow(min, T{1}), max);
    EXPECT_EQ(common::mulIgnoreOverflow(max, T{2}), T{-2});
    EXPECT_EQ(common::negateIgnoreOverflow(min), min);

    EXPECT_TRUE(addWraps(max));
    EXPECT_TRUE(subWraps(min));
    EXPECT_TRUE(mulWraps(max));
    EXPECT_TRUE(negateWraps(min));
}

}

GTEST_TEST(ArithmeticOverflow, IgnoreOverflowWraparound)
{
    /// Builtin integers reproduce the optimizer-sensitive failure. Wide
    /// integers guard the same contract for the shared helper templates.
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int32>();
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int64>();
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int128>();
    arithmetic_overflow_test::checkIgnoreOverflowWraparound<Int256>();
}
