#pragma once

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/keyvaluepair/impl/StateHandler.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractor.h>

#include <fmt/core.h>
#include <magic_enum.hpp>

namespace DB
{

template <typename StateHandler>
class CHKeyValuePairExtractor : public KeyValuePairExtractor
{
    using Key = typename StateHandler::KeyType;
    using Value = typename StateHandler::ValueType;

    using State = typename DB::extractKV::StateHandler::State;
    using NextState = DB::extractKV::StateHandler::NextState;

public:
    CHKeyValuePairExtractor(StateHandler state_handler_)
        : state_handler(std::move(state_handler_))
    {}

    uint64_t extract(const std::string & data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) override
    {
        return extract(std::string_view {data}, keys, values);
    }

    uint64_t extract(std::string_view data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) override
    {
//        std::cerr << "CHKeyValuePairExtractor::extract: \"" << data << "\"" << std::endl;
        auto state =  State::WAITING_KEY;

        Key key;
        Value value;

        uint64_t row_offset = 0;
        const auto & config = state_handler.extractor_configuration;
        std::cerr << "CHKeyValuePairExtractor::extract with "
                  << typeid(state_handler).name()
                  << "\nConfiguration"
                  << "\n\tKV delimiter: '" << config.key_value_delimiter << "'"
                  << "\n\tquote char  : '" << config.quoting_character << "'"
                  << "\n\tpair delims : " << fmt::format("['{}']", fmt::join(config.pair_delimiters, "', '"))
                  << std::endl;

        NextState next_state = {0, state};
        while (state != State::END)
        {
            std::cerr << "CHKeyValuePairExtractor::extract 1, state: "
                      << magic_enum::enum_name(state)
                      << " (" << data.size() << ") "
                      << fancyQuote(data)
                      << std::endl;

            next_state = processState(data, state, key, value, keys, values, row_offset);

            std::cerr << "CHKeyValuePairExtractor::extract 2, new_state: "
                      << magic_enum::enum_name(next_state.state)
                      << " consumed chars: (" << next_state.position_in_string << ") "
                      << fancyQuote(data.substr(0, std::min(data.size(), next_state.position_in_string)))
                      << std::endl;

            if (next_state.position_in_string > data.size() && next_state.state != State::END) {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Attempt to move read poiter past end of available data, from state {} to new state: {}, new position: {}, available data: {}",
                        magic_enum::enum_name(state), magic_enum::enum_name(next_state.state),
                        next_state.position_in_string, data.size());
//                next_result =  {data.size(), State::END};
            }

            data.remove_prefix(next_state.position_in_string);
            state =  next_state.state;

            // No state expects empty input
            if (data.size() == 0)
                break;
        }

        // if break occured earlier, consume previously generated pair
        if (state == State::FLUSH_PAIR || !(key.empty() && value.empty()))
            flushPair(data, key, value, keys, values, row_offset);

        return row_offset;
    }

private:

    NextState processState(std::string_view file, State state, Key & key,
                           Value & value, ColumnString::MutablePtr & keys,
                           ColumnString::MutablePtr & values, uint64_t & row_offset)
    {
        switch (state)
        {
            case State::WAITING_KEY:
                return state_handler.waitKey(file);
            case State::READING_KEY:
            {
                auto result =  state_handler.readKey(file, key);
                std::cerr << "CHKeyValuePairExtractor::processState key: " << fancyQuote(key) << std::endl;
                return result;
            }
            case State::READING_QUOTED_KEY:
            {
                auto result =  state_handler.readQuotedKey(file, key);
                std::cerr << "CHKeyValuePairExtractor::processState key: " << fancyQuote(key) << std::endl;
                return result;
            }
            case State::READING_KV_DELIMITER:
                return state_handler.readKeyValueDelimiter(file);
            case State::WAITING_VALUE:
            {
                return state_handler.waitValue(file);
            }
            case State::READING_VALUE:
            {
                auto result =  state_handler.readValue(file, value);
                std::cerr << "CHKeyValuePairExtractor::processState value: " << fancyQuote(value) << std::endl;
                return result;
            }
            case State::READING_QUOTED_VALUE:
            {
                auto result =  state_handler.readQuotedValue(file, value);
                std::cerr << "CHKeyValuePairExtractor::processState value: " << fancyQuote(value) << std::endl;
                return result;
            }
            case State::FLUSH_PAIR:
                return flushPair(file, key, value, keys, values, row_offset);

            case State::END:
                return {0, state};
        }
    }

    NextState flushPair(const std::string_view & file, Key & key,
                               Value & value, ColumnString::MutablePtr & keys,
                               ColumnString::MutablePtr & values, uint64_t & row_offset)
    {
        std::cerr << "CHKeyValuePairExtractor::flushPair key: " << fancyQuote(key) << ", value: " << fancyQuote(value) << std::endl;
        keys->insertData(key.data(), key.size());
        values->insertData(value.data(), value.size());

        key = {};
        value = {};

        ++row_offset;
        std::cerr << "CHKeyValuePairExtractor::flushPair total pairs: " << row_offset << std::endl;

        return {0, file.size() == 0 ? State::END : State::WAITING_KEY};
    }

    StateHandler state_handler;
};

}
