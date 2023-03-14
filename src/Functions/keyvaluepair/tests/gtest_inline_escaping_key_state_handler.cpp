#include <Functions/keyvaluepair/src/impl/state/strategies/escaping/InlineEscapingKeyStateHandler.h>
#include <gtest/gtest.h>

namespace DB
{

void test_wait(const InlineEscapingKeyStateHandler & handler, std::string_view input, std::size_t expected_pos, State expected_state)
{
    auto next_state = handler.wait(input, 0u);

    ASSERT_EQ(next_state.position_in_string, expected_pos);
    ASSERT_EQ(next_state.state, expected_state);
}

template <bool enclosed>
void test_read(const InlineEscapingKeyStateHandler & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    NextState next_state;
    std::string element;

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

void test_read(const InlineEscapingKeyStateHandler & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    test_read<false>(handler, input, expected_element, expected_pos, expected_state);
}

void test_read_enclosed(const InlineEscapingKeyStateHandler & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    test_read<true>(handler, input, expected_element, expected_pos, expected_state);
}

TEST(InlineEscapingKeyStateHandler, Wait)
{
    auto pair_delimiters = std::vector<char>{',', ' '};
    auto quoting_characters = std::vector<char>{'"'};

    ExtractorConfiguration configuration(':', pair_delimiters, quoting_characters);
    InlineEscapingKeyStateHandler handler(configuration);

    test_wait(handler, "name", 0u, READING_KEY);
    test_wait(handler, "\\:name", 2u, READING_KEY);
    test_wait(handler, R"(\\"name)", 3u, READING_ENCLOSED_KEY);

    test_wait(handler, "", 0u, END);
    test_wait(handler, "\\\\", 2u, END);
}

TEST(InlineEscapingKeyStateHandler, Read)
{
    auto enclosing_character = '"';

    auto pair_delimiters = std::vector<char>{',', ' '};
    auto quoting_characters = std::vector<char>{enclosing_character};

    ExtractorConfiguration configuration(':', pair_delimiters, quoting_characters);

    InlineEscapingKeyStateHandler handler(configuration);

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

TEST(InlineEscapingKeyStateHandler, ReadEnclosed)
{
    auto pair_delimiters = std::vector<char>{',', ' '};
    auto quoting_characters = std::vector<char>{'"'};

    ExtractorConfiguration configuration(':', pair_delimiters, quoting_characters);
    InlineEscapingKeyStateHandler handler(configuration);

    std::string regular_key = "name";
    std::string regular_key_with_end_quote = regular_key + "\"";
    std::string key_with_special_characters = "name $!@#¨%&*%&%.569-519";
    std::string key_with_special_characters_with_end_quote = "name $!@#¨%&*%&%.569-519\"";

    std::string key_with_escape_character = regular_key + R"(\n\x4E")";

    test_read_enclosed(handler, regular_key, "", regular_key.size(), END);
    test_read_enclosed(handler, regular_key_with_end_quote, regular_key, regular_key_with_end_quote.size(), READING_KV_DELIMITER);
    test_read_enclosed(handler, key_with_special_characters_with_end_quote, key_with_special_characters, key_with_special_characters_with_end_quote.size(), READING_KV_DELIMITER);
    test_read_enclosed(handler, key_with_escape_character, regular_key + "\nN", key_with_escape_character.size(), READING_KV_DELIMITER);
}

}
