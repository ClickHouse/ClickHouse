#include "InlineKeyValuePairExtractor.h"

namespace DB
{

InlineKeyValuePairExtractor::InlineKeyValuePairExtractor(InlineEscapingKeyStateHandler key_state_handler_,
                                                         InlineEscapingValueStateHandler value_state_handler_)
    : key_state_handler(key_state_handler_), value_state_handler(value_state_handler_) {}

InlineKeyValuePairExtractor::Response InlineKeyValuePairExtractor::extract(const std::string & data)
{
    auto view = std::string_view {data.begin(), data.end()};
    return extract(view);
}

InlineKeyValuePairExtractor::Response InlineKeyValuePairExtractor::extract(std::string_view data)
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

NextState InlineKeyValuePairExtractor::processState(std::string_view file, std::size_t pos, State state,
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

NextState InlineKeyValuePairExtractor::flushPair(const std::string_view & file, std::size_t pos,
                                                 std::string & key, std::string & value, Response & response)
{
    response[std::move(key)] = std::move(value);

    key.clear();
    value.clear();

    return {pos, pos == file.size() ? State::END : State::WAITING_KEY};
}

}
