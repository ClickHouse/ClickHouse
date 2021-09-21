#pragma once

#include <gtest/gtest.h>
#include <common/EnumReflection.h>

enum class Foo : int { Bar = 0, Baz = 1, other = 2, CAPS = 3, snake_case = 4};

TEST(EnumReflection, enumPrinting)
{
    EXPECT_EQ(fmt::to_string(Foo::Bar), "Bar");
    EXPECT_EQ(fmt::to_string(Foo::Baz), "Baz");
    EXPECT_EQ(fmt::to_string(Foo::other), "other");
    EXPECT_EQ(fmt::to_string(Foo::CAPS), "CAPS");
    EXPECT_EQ(fmt::to_string(Foo::snake_case), "snake_case");

    EXPECT_EQ(fmt::format("{} value", Foo::Bar), "Bar value");
}

template <int I>
constexpr int foo() { return I; }

TEST(EnumReflection, enumIteration)
{
    int sum = 0;

    static_for<Foo>([&sum](auto constant) { sum += foo<constant>(); });

    EXPECT_EQ(sum, 10);
}
