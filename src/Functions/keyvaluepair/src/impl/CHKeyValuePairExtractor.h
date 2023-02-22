#pragma once

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include "state/StateHandler.h"
#include "../KeyValuePairExtractor.h"


namespace DB
{

template <CKeyStateHandler KeyStateHandler, CValueStateHandler ValueStateHandler>
class CHKeyValuePairExtractor : public KeyValuePairExtractor
{
    using Key = typename KeyStateHandler::ElementType;
    using Value = typename ValueStateHandler::ElementType;

public:
    CHKeyValuePairExtractor(KeyStateHandler key_state_handler_, ValueStateHandler value_state_handler_)
        : key_state_handler(std::move(key_state_handler_)), value_state_handler(std::move(value_state_handler_))
    {}

    uint64_t extract(const std::string & data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) override
    {
        return extract(std::string_view {data}, keys, values);
    }

    uint64_t extract(std::string_view data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) override
    {
        auto state = State::WAITING_KEY;

        std::size_t pos = 0;

        Key key;
        Value value;

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

    NextState processState(std::string_view file, std::size_t pos, State state, Key & key,
                           Value & value, ColumnString::MutablePtr & keys,
                           ColumnString::MutablePtr & values, uint64_t & row_offset)
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

    NextState flushPair(const std::string_view & file, std::size_t pos, Key & key,
                               Value & value, ColumnString::MutablePtr & keys,
                               ColumnString::MutablePtr & values, uint64_t & row_offset)
    {
        keys->insertData(key.data(), key.size());
        values->insertData(value.data(), value.size());

        key = {};
        value = {};

        row_offset++;

        return {pos, pos == file.size() ? State::END : State::WAITING_KEY};
    }

    KeyStateHandler key_state_handler;
    ValueStateHandler value_state_handler;
};

}
