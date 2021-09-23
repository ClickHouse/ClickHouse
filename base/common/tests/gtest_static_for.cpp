#include <array>
#include <gtest/gtest.h>
#include <common/static_for.h>

template <int I> constexpr int foo() { return I; }

TEST(StaticFor, Basic)
{
    int sum = 0;

    static_for<0, 6>([&sum](auto constant) { sum += foo<constant>(); return false; });

    EXPECT_EQ(sum, 15);
}

TEST(StaticFor, WithStop)
{
    int sum = 0;

    static_for<0, 6>([&sum](auto constant) { sum += foo<constant>(); return sum >= 10; });

    EXPECT_EQ(sum, 10);
}

TEST(StaticFor, WithConstexprStop)
{
    int sum = 0;

    static_for<0, 6>([&sum](auto constant)
    {
        sum += foo<constant>();

        if constexpr (constant < 4) //NOLINT
            return false; //NOLINT
        else
            return true;
    });

    EXPECT_EQ(sum, 10);
}

TEST(StaticFor, WithSingleIteration)
{
    int sum = 0;

    static_for<0, 256>([&sum](auto constant) { sum += foo<constant>(); return true; });

    EXPECT_EQ(sum, 0);
}

TEST(StaticFor, WithContainer)
{
    int sum = 0;

    constexpr auto arr = MakeCTArray(0, 1, 2, 3, 4, 5);

    static_for<arr>([&sum](auto constant) { sum += foo<constant>(); return false; });

    EXPECT_EQ(sum, 15);
}
