#pragma once

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include "state/InlineEscapingKeyStateHandler.h"
#include "state/InlineEscapingValueStateHandler.h"


namespace DB
{

template <CInlineEscapingKeyStateHandler InlineKeyStateHandler, CInlineEscapingValueStateHandler InlineValueStateHandler>
class CHKeyValuePairExtractor
{
public:
    using Response = ColumnMap::Ptr;

    CHKeyValuePairExtractor(InlineKeyStateHandler key_state_handler_, InlineValueStateHandler value_state_handler_)
        : key_state_handler(std::move(key_state_handler_)), value_state_handler(std::move(value_state_handler_))
    {}

    uint64_t extract(const std::string_view & data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values)
    {
        auto state = State::WAITING_KEY;

        std::size_t pos = 0;

        std::string key;
        std::string value;
        uint64_t row_offset = 0;

        while (state != State::END)
        {
            auto next_state = processState(data, pos, state, key, value, keys, values, row_offset);

            pos = next_state.position_in_string;
            state = next_state.state;
        }

        return row_offset;
    }

private:

    NextState processState(std::string_view file, std::size_t pos, State state, std::string & key,
                           std::string & value, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values, uint64_t & row_offset)
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
                return flushPair(file, pos, key, value, keys, values, row_offset);
            case END:
                return {pos, state};
        }
    }

    static NextState flushPair(const std::string_view & file, std::size_t pos, std::string & key,
                        std::string & value, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values, uint64_t & row_offset)
    {
        keys->insert(std::move(key));
        values->insert(std::move(value));

        key = {};
        value = {};

        row_offset++;

        return {pos, pos == file.size() ? State::END : State::WAITING_KEY};
    }

    InlineKeyStateHandler key_state_handler;
    InlineValueStateHandler value_state_handler;
};

}
