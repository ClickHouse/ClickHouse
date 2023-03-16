#include <Functions/keyvaluepair/src/impl/state/strategies/noescaping/NoEscapingKeyStateHandler.h>
#include <gtest/gtest.h>

namespace DB
{

void test_wait(const auto & handler, std::string_view input, std::size_t expected_pos, State expected_state)
{
    auto next_state = handler.wait(input, 0u);

    ASSERT_EQ(next_state.position_in_string, expected_pos);
    ASSERT_EQ(next_state.state, expected_state);
}

template <bool enclosed>
void test_read(const auto & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    NextState next_state;
    std::string_view element;

    if constexpr (enclosed)
    {
        next_state = handler.readEnclosed(input, 0u, element);
    }
    else
    {
        next_state = handler.read(input, 0u, element);
    }

    ASSERT_EQ(next_state.position_in_string, expected_pos);
    ASSERT_EQ(next_state.state, expected_state);
    ASSERT_EQ(element, expected_element);
}

void test_read(const auto & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    test_read<false>(handler, input, expected_element, expected_pos, expected_state);
}

void test_read_enclosed(const auto & handler, std::string_view input, std::string_view expected_element,
                        std::size_t expected_pos, State expected_state)
{
    test_read<true>(handler, input, expected_element, expected_pos, expected_state);
}

TEST(NoEscapingKeyStateHandler, Wait)
{
    auto pair_delimiters = std::vector<char>{',', ' ', '$'};

    auto configuration = ConfigurationFactory::createWithEscaping(':', '"', pair_delimiters);

    NoEscapingKeyStateHandler handler(configuration);

    test_wait(handler, "name", 0u, READING_KEY);
    test_wait(handler, "\\:name", 0u, READING_KEY);
    // quoted expected pos is + 1 because as of now it is skipped, maybe I should change it
    test_wait(handler, "\"name", 1u, READING_ENCLOSED_KEY);

    test_wait(handler, ", $name", 3u, READING_KEY);
    test_wait(handler, ", $\"name", 4u, READING_ENCLOSED_KEY);

    test_wait(handler, "", 0u, END);
}

TEST(NoEscapingKeyStateHandler, Read)
{
    auto pair_delimiters = std::vector<char>{',', ' '};

    auto configuration = ConfigurationFactory::createWithEscaping(':', '"', pair_delimiters);

    NoEscapingKeyStateHandler handler(configuration);

    std::string key_str = "name";
    std::string key_with_delimiter_str = key_str + ':';
    std::string key_with_delimiter_and_left_spacing = "  " + key_with_delimiter_str;
    std::string key_with_delimiter_and_random_characters_str = key_str + ':' + "a$a\\:''\"";

    // no delimiter, should discard
    test_read(handler, key_str, "", key_str.size(), END);

    // valid
    test_read(handler, key_with_delimiter_str, key_str, key_with_delimiter_str.size(), WAITING_VALUE);

    // valid as well
    test_read(handler, key_with_delimiter_and_random_characters_str, key_str, key_with_delimiter_str.size(), WAITING_VALUE);

    test_read(handler, "", "", 0u, END);
}

}
