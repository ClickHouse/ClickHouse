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
    auto next_state = handler.waitValue(input);

    ASSERT_EQ(next_state.position_in_string, expected_pos);
    ASSERT_EQ(next_state.state, expected_state);
}

}

TEST(extractKVPairInlineEscapingValueStateHandler, Wait)
{
    auto pair_delimiters = std::vector<char> {','};

    auto configuration = ConfigurationFactory::createWithEscaping(':', '"', pair_delimiters, Configuration::UnexpectedQuotingCharacterStrategy::PROMOTE);
    StateHandlerImpl<true> handler(configuration);

    test_wait(handler, " los$ yours3lf", 0u, State::READING_VALUE);
}
