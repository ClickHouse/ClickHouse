#pragma once

#include <Functions/keyvaluepair/src/KeyValuePairExtractor.h>
#include "state/InlineEscapingKeyStateHandler.h"
#include "state/InlineEscapingValueStateHandler.h"
#include "state/State.h"

namespace DB
{

template <CInlineEscapingKeyStateHandler InlineKeyStateHandler, CInlineEscapingValueStateHandler InlineValueStateHandler>
class InlineKeyValuePairExtractor : public KeyValuePairExtractor<std::unordered_map<std::string, std::string>>
{
public:
    InlineKeyValuePairExtractor(InlineKeyStateHandler key_state_handler_, InlineValueStateHandler value_state_handler_)
        : key_state_handler(key_state_handler_), value_state_handler(value_state_handler_) {}

    Response extract(const std::string & data) override
    {
        auto view = std::string_view {data.begin(), data.end()};
        return extract(view);
    }

    Response extract(std::string_view data) override
    {
        InlineKeyValuePairExtractor::Response response;

        auto state = State::WAITING_KEY;

        std::size_t pos = 0;

        std::string key;
        std::string value;

        while (state != State::END)
        {
            auto next_state = processState(data, pos, state, key, value, response);

            pos = next_state.position_in_string;
            state = next_state.state;
        }

        return response;
    }

private:
    NextState processState(std::string_view file, std::size_t pos, State state,
                           std::string & key, std::string & value, Response & response)
    {
        switch (state)
        {
            case State::WAITING_KEY:
                return key_state_handler.wait(file, pos);
            case State::READING_KEY:
                return key_state_handler.read(file, pos, key);
            case State::READING_ENCLOSED_KEY:
                return key_state_handler.readEnclosed(file, pos, key);
            case State::READING_KV_DELIMITER:
                return key_state_handler.readKeyValueDelimiter(file, pos);
            case State::WAITING_VALUE:
                return value_state_handler.wait(file, pos);
            case State::READING_VALUE:
                return value_state_handler.read(file, pos, value);
            case State::READING_ENCLOSED_VALUE:
                return value_state_handler.readEnclosed(file, pos, value);
            case State::READING_EMPTY_VALUE:
                return value_state_handler.readEmpty(file, pos, value);
            case State::FLUSH_PAIR:
                return flushPair(file, pos, key, value, response);
            case END:
                return {pos, state};
        }
    }

    static NextState flushPair(const std::string_view & file, std::size_t pos, std::string & key,
                               std::string & value, Response & response)
    {
        response[std::move(key)] = std::move(value);

        key.clear();
        value.clear();

        return {pos, pos == file.size() ? State::END : State::WAITING_KEY};
    }

    InlineKeyStateHandler key_state_handler;
    InlineValueStateHandler value_state_handler;
};

}
