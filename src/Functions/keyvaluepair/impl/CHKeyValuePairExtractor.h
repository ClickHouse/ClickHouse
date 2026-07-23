#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/keyvaluepair/impl/StateHandler.h>
#include <Functions/keyvaluepair/impl/StateHandlerImpl.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int LIMIT_EXCEEDED;
}

namespace extractKV
{
/*
 * Handle state transitions and a few states like `FLUSH_PAIR` and `END`.
 * */
template <typename StateHandler>
class KeyValuePairExtractor
{
    using State = typename DB::extractKV::StateHandler::State;
    using NextState = DB::extractKV::StateHandler::NextState;

public:
    using PairWriter = typename StateHandler::PairWriter;

    KeyValuePairExtractor(const Configuration & configuration_, uint64_t max_number_of_pairs_)
        : state_handler(StateHandler(configuration_))
        , max_number_of_pairs(max_number_of_pairs_)
    {
    }

protected:
    uint64_t extractImpl(std::string_view data, typename StateHandler::PairWriter & pair_writer)
    {
        auto state =  State::WAITING_KEY;

        uint64_t row_offset = 0;

        while (state != State::END)
        {
            auto next_state = processState(data, state, pair_writer, row_offset);

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
        reset(pair_writer);

        return row_offset;
    }

private:
    void incrementRowOffset(uint64_t & row_offset)
    {
        row_offset++;

        if (row_offset > max_number_of_pairs)
        {
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Number of pairs produced exceeded the limit of {}", max_number_of_pairs);
        }
    }

    NextState processState(std::string_view file, State state, auto & pair_writer, uint64_t & row_offset)
    {
        switch (state)
        {
            case State::WAITING_KEY:
            {
                return state_handler.waitKey(file);
            }
            case State::READING_KEY:
            {
                return state_handler.readKey(file, pair_writer);
            }
            case State::READING_QUOTED_KEY:
            {
                return state_handler.readQuotedKey(file, pair_writer);
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
                return state_handler.readValue(file, pair_writer);
            }
            case State::READING_QUOTED_VALUE:
            {
                return state_handler.readQuotedValue(file, pair_writer);
            }
            case State::FLUSH_PAIR:
            {
                incrementRowOffset(row_offset);
                return state_handler.flushPair(file, pair_writer);
            }
            case State::FLUSH_PAIR_AFTER_QUOTED_VALUE:
            {
                incrementRowOffset(row_offset);
                return state_handler.flushPairAfterQuotedValue(file, pair_writer);
            }
            case State::WAITING_PAIR_DELIMITER:
            {
                return state_handler.waitPairDelimiter(file);
            }
            case State::END:
            {
                return {0, state};
            }
        }
    }

    void reset(auto & pair_writer)
    {
        pair_writer.resetKey();
        pair_writer.resetValue();
    }

    StateHandler state_handler;
    uint64_t max_number_of_pairs;
};

}

struct KeyValuePairExtractorNoEscaping : extractKV::KeyValuePairExtractor<extractKV::NoEscapingStateHandler>
{
    using StateHandler = extractKV::NoEscapingStateHandler;
    explicit KeyValuePairExtractorNoEscaping(const extractKV::Configuration & configuration_, std::size_t max_number_of_pairs_)
        : KeyValuePairExtractor(configuration_, max_number_of_pairs_) {}

    uint64_t extract(std::string_view data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values)
    {
        auto pair_writer = typename StateHandler::PairWriter(*keys, *values);
        return extractImpl(data, pair_writer);
    }
};

struct KeyValuePairExtractorInlineEscaping : extractKV::KeyValuePairExtractor<extractKV::InlineEscapingStateHandler>
{
    using StateHandler = extractKV::InlineEscapingStateHandler;
    explicit KeyValuePairExtractorInlineEscaping(const extractKV::Configuration & configuration_, std::size_t max_number_of_pairs_)
        : KeyValuePairExtractor(configuration_, max_number_of_pairs_) {}

    uint64_t extract(std::string_view data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values)
    {
        auto pair_writer = typename StateHandler::PairWriter(*keys, *values);
        return extractImpl(data, pair_writer);
    }
};

struct KeyValuePairExtractorReferenceMap : extractKV::KeyValuePairExtractor<extractKV::ReferencesMapStateHandler>
{
    using StateHandler = extractKV::ReferencesMapStateHandler;
    explicit KeyValuePairExtractorReferenceMap(const extractKV::Configuration & configuration_, std::size_t max_number_of_pairs_)
        : KeyValuePairExtractor(configuration_, max_number_of_pairs_) {}

    uint64_t extract(std::string_view data, std::map<std::string_view, std::string_view> & map)
    {
        auto pair_writer = typename StateHandler::PairWriter(map);
        return extractImpl(data, pair_writer);
    }
};

}
