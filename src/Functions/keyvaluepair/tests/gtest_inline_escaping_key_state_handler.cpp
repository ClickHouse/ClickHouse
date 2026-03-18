#include <Columns/ColumnString.h>
#include <Functions/keyvaluepair/impl/StateHandlerImpl.h>


#include <Functions/keyvaluepair/impl/StateHandler.h>
#include <gtest/gtest.h>

namespace
{

using namespace DB;
using namespace DB::extractKV;

using State = extractKV::StateHandler::State;
using NextState = extractKV::StateHandler::NextState;

void test_wait(const auto & handler, std::string_view input, std::size_t expected_pos, State expected_state)
{
    auto next_state = handler.waitKey(input);

    ASSERT_EQ(next_state.position_in_string, expected_pos);
    ASSERT_EQ(next_state.state, expected_state);
}

template <bool quoted>
void test_read(const auto & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    auto str = ColumnString::create();
    NextState next_state;
    InlineEscapingStateHandler::StringWriter element(*str);

    if constexpr (quoted)
    {
        next_state = handler.readQuotedKey(input, element);
    }
    else
    {
        next_state = handler.readKey(input, element);
    }

    ASSERT_EQ(next_state.position_in_string, expected_pos);
    ASSERT_EQ(next_state.state, expected_state);
    ASSERT_EQ(element.uncommittedChunk(), expected_element);
}

void test_read(const auto & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    test_read<false>(handler, input, expected_element, expected_pos, expected_state);
}

void test_read_quoted(const auto & handler, std::string_view input, std::string_view expected_element,
               std::size_t expected_pos, State expected_state)
{
    test_read<true>(handler, input, expected_element, expected_pos, expected_state);
}

}

TEST(extractKVPairInlineEscapingKeyStateHandler, Wait)
{
    auto pair_delimiters = std::vector<char>{',', ' '};

    auto configuration = ConfigurationFactory::createWithEscaping(':', '"', pair_delimiters);

    StateHandlerImpl<true> handler(configuration);

    test_wait(handler, "name", 0u, State::READING_KEY);
    test_wait(handler, "\\:name", 2u, State::READING_KEY);
    test_wait(handler, R"(\\"name)", 3u, State::READING_QUOTED_KEY);

    test_wait(handler, "", 0u, State::END);
    test_wait(handler, "\\\\", 2u, State::END);
}

TEST(extractKVPairInlineEscapingKeyStateHandler, Read)
{
    auto pair_delimiters = std::vector<char>{',', ' '};

    auto configuration = ConfigurationFactory::createWithEscaping(':', '"', pair_delimiters);

    StateHandlerImpl<true> handler(configuration);

    std::string key_str = "name";
    std::string key_with_delimiter_str = key_str + ':';
    std::string key_with_delimiter_and_random_characters_str = key_str + ':' + "a$a\\:''\"";

    // no delimiter, should discard
    test_read(handler, key_str, "", key_str.size(), State::END);

    // valid
    test_read(handler, key_with_delimiter_str, key_str, key_with_delimiter_str.size(), State::WAITING_VALUE);

    // valid as well
    test_read(handler, key_with_delimiter_and_random_characters_str, key_str, key_with_delimiter_str.size(), State::WAITING_VALUE);

    test_read(handler, "", "", 0u, State::END);
}

TEST(extractKVPairInlineEscapingKeyStateHandler, ReadEnclosed)
{
    auto pair_delimiters = std::vector<char>{',', ' '};

    auto configuration = ConfigurationFactory::createWithEscaping(':', '"', pair_delimiters);

    StateHandlerImpl<true> handler(configuration);

    std::string regular_key = "name";
    std::string regular_key_with_end_quote = regular_key + "\"";
    std::string key_with_special_characters = "name $!@#¨%&*%&%.569-519";
    std::string key_with_special_characters_with_end_quote = "name $!@#¨%&*%&%.569-519\"";

    std::string key_with_escape_character = regular_key + R"(\n\x4E")";

    test_read_quoted(handler, regular_key, "", regular_key.size(), State::END);
    test_read_quoted(handler, regular_key_with_end_quote, regular_key, regular_key_with_end_quote.size(), State::READING_KV_DELIMITER);
    test_read_quoted(handler, key_with_special_characters_with_end_quote, key_with_special_characters, key_with_special_characters_with_end_quote.size(), State::READING_KV_DELIMITER);
    test_read_quoted(handler, key_with_escape_character, regular_key + "\nN", key_with_escape_character.size(), State::READING_KV_DELIMITER);
}
