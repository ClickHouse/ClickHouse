#include <gtest/gtest.h>
#include <Functions/keyvaluepair/src/impl/state/util/CharacterFinder.h>
#include "base/find_symbols.h"


namespace DB
{
void test_find_first(
    const CharacterFinder& finder, const std::string_view& haystack,
    const std::vector<char>& needles, const std::optional<std::pair<char, CharacterFinder::Position>>& expected_result)
{
    auto pos = finder.find_first(haystack, needles);

    ASSERT_EQ(pos.has_value(), expected_result.has_value());

    if (expected_result)
    {
        auto [expected_character, expected_position] = *expected_result;

        ASSERT_EQ(expected_position, *pos);
        ASSERT_EQ(expected_character, haystack[*pos]);
    }
}

void test_find_first_not(const CharacterFinder& finder, const std::string_view& haystack, const std::vector<char>& needles, const std::optional<std::pair<char, CharacterFinder::Position>>& expected_result)
{
    auto pos = finder.find_first_not(haystack, needles);

    ASSERT_EQ(pos.has_value(), expected_result.has_value());

    if (expected_result)
    {
        auto [expected_character, expected_position] = *expected_result;

        ASSERT_EQ(expected_position, *pos);
        ASSERT_EQ(expected_character, haystack[*pos]);
    }
}

TEST(CharacterFinderTest, FindFirst)
{
    CharacterFinder finder;

    // Test empty haystack
    std::string_view empty;
    std::vector<char> needles = {'a', 'b', 'c'};
    test_find_first(finder, empty, needles, std::nullopt);

    // Test haystack with no needles
    std::string_view no_needles = "hello world";
    std::vector<char> empty_needles = {};
    test_find_first(finder, no_needles, empty_needles, std::nullopt);

    // Test haystack with a single needle
    std::string_view single_needle = "hello world";
    std::vector<char> single_needles = {'o'};
    test_find_first(finder, single_needle, single_needles, std::make_optional<std::pair<char, CharacterFinder::Position>>('o', 4));

    // Test haystack with multiple needles
    std::string_view multiple_needles = "hello world";
    std::vector<char> all_needles = {'a', 'e', 'i', 'o', 'u'};
    test_find_first(finder, multiple_needles, all_needles, std::make_optional<std::pair<char, CharacterFinder::Position>>('e', 1));

    // Test haystack with special characters
    std::string_view special_needle = R"(this=that\other"thing")";
    std::vector<char> special_needles = {'=', '\\', '"'};
    test_find_first(finder, special_needle, special_needles, std::make_optional<std::pair<char, CharacterFinder::Position>>('=', 4));
}

TEST(CharacterFinderTest, FindFirstNot)
{
//    CharacterFinder finder;

//     Test empty haystack
//    std::string_view empty;
//    std::vector<char> needles = {'a', 'b', 'c'};
//    test_find_first_not(finder, empty, needles, std::nullopt);
//
//    // Test haystack with no needles
//    std::string_view no_needles = "hello world";
//    std::vector<char> empty_needles = {};
//    test_find_first_not(finder, no_needles, empty_needles, std::make_optional<std::pair<char, CharacterFinder::Position>>('h', 0));
//
//    // Test haystack with a single needle
//    std::string_view single_needle = "hello world";
//    std::vector<char> single_needles = {'o'};
//    test_find_first_not(finder, single_needle, single_needles, std::make_optional<std::pair<char, CharacterFinder::Position>>('h', 0));

//     Test haystack with multiple needles
//    std::string_view multiple_needles = "hello world";
//    std::vector<char> all_needles = {'h', 'e', 'l', 'o', 'w', 'r', 'd'};
//    test_find_first_not(finder, multiple_needles, all_needles, std::nullopt);

    // 17 byte long string, only loop takes place
    std::string_view multiple_needles_haystack = "helloworldddddd";
    const auto * res = find_first_not_symbols<'h', 'e', 'l', 'o', 'w', 'r', 'd'>(multiple_needles_haystack.begin(), multiple_needles_haystack.end());

    ASSERT_EQ(res, multiple_needles_haystack.end());
}

}
