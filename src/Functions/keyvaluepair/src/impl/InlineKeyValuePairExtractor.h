#pragma once

#include <Functions/keyvaluepair/src/KeyValuePairExtractor.h>
#include "state/InlineEscapingKeyStateHandler.h"
#include "state/InlineEscapingValueStateHandler.h"
#include "state/State.h"

namespace DB
{

class InlineKeyValuePairExtractor : public KeyValuePairExtractor<std::unordered_map<std::string, std::string>>
{
public:
    InlineKeyValuePairExtractor(InlineEscapingKeyStateHandler key_state_handler, InlineEscapingValueStateHandler value_state_handler);

    Response extract(const std::string & data) override;

    Response extract(std::string_view data) override;

private:
    NextState processState(std::string_view file, std::size_t pos, State state,
                           std::string & key, std::string & value, Response & response);

    static NextState flushPair(const std::string_view & file, std::size_t pos, std::string & key,
                               std::string & value, Response & response);

    InlineEscapingKeyStateHandler key_state_handler;
    InlineEscapingValueStateHandler value_state_handler;
};

}
