#include <Functions/keyvaluepair/src/impl/state/strategies/escaping/InlineEscapingStateHandler.h>
#include <gtest/gtest.h>

namespace DB
{

void test_wait(const auto & handler, std::string_view input, std::size_t expected_pos, State expected_state)
{
    auto next_state = handler.wait(input);

    ASSERT_EQ(next_state.position_in_string, expected_pos);
    ASSERT_EQ(next_state.state, expected_state);
}

TEST(extractKVPair_InlineEscapingValueStateHandler, Wait)
{
    auto pair_delimiters = std::vector<char> {','};

    auto configuration = ConfigurationFactory::createWithEscaping(':', '"', pair_delimiters);
    InlineEscapingValueStateHandler handler(configuration);

    test_wait(handler, " los$ yours3lf", 0u, READING_VALUE);
}

}
