#include <string_view>

#include <base/StringViewHash.h>

#include <gtest/gtest.h>

using std::string_view_literals::operator""sv;

/// The operators from `StringViewHash.h` take separate paths at run time and under constant
/// evaluation, so check every case both ways.
#define CHECK(expression) \
    static_assert(expression); \
    ASSERT_TRUE(expression)

TEST(StringViewComparison, ConstantEvaluatedAndRunTimePathsAgree)
{
    CHECK("a"sv == "a"sv);
    CHECK("a"sv != "b"sv);
    CHECK("a"sv < "b"sv);
    CHECK("b"sv > "a"sv);

    /// One view a prefix of the other, so only the size breaks the tie.
    CHECK("abc"sv < "abcd"sv);
    CHECK("abcd"sv > "abc"sv);
}

#undef CHECK
