#pragma once

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/keyvaluepair/impl/StateHandler.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int LIMIT_EXCEEDED;
}

/*
 * Handle state transitions and a few states like `FLUSH_PAIR` and `END`.
 * */
template <typename StateHandler>
class CHKeyValuePairExtractor : public KeyValuePairExtractor
{
    using State = typename DB::extractKV::StateHandler::State;
    using NextState = DB::extractKV::StateHandler::NextState;

public:
    explicit CHKeyValuePairExtractor(StateHandler state_handler_, uint64_t max_number_of_pairs_)
        : state_handler(std::move(state_handler_)), max_number_of_pairs(max_number_of_pairs_)
    {}

    uint64_t extract(const std::string & data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) override
    {
        return extract(std::string_view {data}, keys, values);
    }

    uint64_t extract(std::string_view data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) override
    {
        auto state =  State::WAITING_KEY;

        auto key = typename StateHandler::StringWriter(*keys);
        auto value = typename StateHandler::StringWriter(*values);

        uint64_t row_offset = 0;

        while (state != State::END)
        {
            auto next_state = processState(data, state, key, value, row_offset);

            if (next_state.position_in_string > data.size() && next_state.state != State::END)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Attempt to move read pointer past end of available data, from state {} to new state: {}, new position: {}, available data: {}",
                        magic_enum::enum_name(state), magic_enum::enum_name(next_state.state),
                        next_state.position_in_string, data.size());
            }

            data.remove_prefix(next_state.position_in_string);
            state = next_state.state;
        }

        // below reset discards invalid keys and values
        reset(key, value);

        return row_offset;
    }

private:

    NextState processState(std::string_view file, State state, auto & key, auto & value, uint64_t & row_offset)
    {
        switch (state)
        {
            case State::WAITING_KEY:
            {
                return state_handler.waitKey(file);
            }
            case State::READING_KEY:
            {
                return state_handler.readKey(file, key);
            }
            case State::READING_QUOTED_KEY:
            {
                return state_handler.readQuotedKey(file, key);
            }
            case State::READING_KV_DELIMITER:
            {
                return state_handler.readKeyValueDelimiter(file);
            }
            case State::WAITING_VALUE:
            {
                return state_handler.waitValue(file);
            }
            case State::READING_VALUE:
            {
                return state_handler.readValue(file, value);
            }
            case State::READING_QUOTED_VALUE:
            {
                return state_handler.readQuotedValue(file, value);
            }
            case State::FLUSH_PAIR:
            {
                return flushPair(file, key, value, row_offset);
            }
            case State::END:
            {
                return {0, state};
            }
        }
    }

    NextState flushPair(const std::string_view & file, auto & key,
                        auto & value, uint64_t & row_offset)
    {
        row_offset++;

        if (row_offset > max_number_of_pairs)
        {
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Number of pairs produced exceeded the limit of {}", max_number_of_pairs);
        }

        key.commit();
        value.commit();

        return {0, file.empty() ? State::END : State::WAITING_KEY};
    }

    void reset(auto & key, auto & value)
    {
        key.reset();
        value.reset();
    }

    StateHandler state_handler;
    uint64_t max_number_of_pairs;
};

}
