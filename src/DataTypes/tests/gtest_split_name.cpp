#include <DataTypes/NestedUtils.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(SplitName, forward)
{
    ASSERT_EQ(Nested::splitName("abc"), (std::pair<std::string, std::string>{"abc", ""}));
    ASSERT_EQ(Nested::splitName("a.b"), (std::pair<std::string, std::string>{"a", "b"}));
    ASSERT_EQ(Nested::splitName("a.b.c"), (std::pair<std::string, std::string>{"a", "b.c"}));
    ASSERT_EQ(Nested::splitName("a.1"), (std::pair<std::string, std::string>{"a.1", ""}));
    ASSERT_EQ(Nested::splitName("a.1.b"), (std::pair<std::string, std::string>{"a.1.b", ""}));
    ASSERT_EQ(Nested::splitName("1.a"), (std::pair<std::string, std::string>{"1.a", ""}));
    ASSERT_EQ(Nested::splitName("a.b1.b2"), (std::pair<std::string, std::string>{"a", "b1.b2"}));
    ASSERT_EQ(Nested::splitName("a.b1.2a.3a"), (std::pair<std::string, std::string>{"a.b1.2a.3a", ""}));
    ASSERT_EQ(Nested::splitName(".."), (std::pair<std::string, std::string>{"..", ""}));
}

TEST(SplitName, reverse)
{
    ASSERT_EQ(Nested::splitName("abc", true), (std::pair<std::string, std::string>{"abc", ""}));
    ASSERT_EQ(Nested::splitName("a.b", true), (std::pair<std::string, std::string>{"a", "b"}));
    ASSERT_EQ(Nested::splitName("a.b.c", true), (std::pair<std::string, std::string>{"a.b", "c"}));
    ASSERT_EQ(Nested::splitName("a.1", true), (std::pair<std::string, std::string>{"a.1", ""}));
    ASSERT_EQ(Nested::splitName("a.1a.b", true), (std::pair<std::string, std::string>{"a.1a.b", ""}));
    ASSERT_EQ(Nested::splitName("1a.b", true), (std::pair<std::string, std::string>{"1a.b", ""}));
    ASSERT_EQ(Nested::splitName("a.b1.b2", true), (std::pair<std::string, std::string>{"a.b1", "b2"}));
    ASSERT_EQ(Nested::splitName("a.b1.2a.3a", true), (std::pair<std::string, std::string>{"a.b1.2a.3a", ""}));
    ASSERT_EQ(Nested::splitName("a.b1.b2.b3", true), (std::pair<std::string, std::string>{"a.b1.b2", "b3"}));
    ASSERT_EQ(Nested::splitName("..", true), (std::pair<std::string, std::string>{"..", ""}));
}
