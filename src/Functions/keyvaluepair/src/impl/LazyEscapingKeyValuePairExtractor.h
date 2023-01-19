#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <Functions/keyvaluepair/src/KeyValuePairEscapingProcessor.h>
#include <Functions/keyvaluepair/src/KeyValuePairExtractor.h>
#include <Functions/keyvaluepair/src/impl/state/KeyStateHandler.h>
#include <Functions/keyvaluepair/src/impl/state/ValueStateHandler.h>

namespace DB
{

/*
 * Implements key value pair extraction by ignoring escaping and deferring its processing to the end.
 * This strategy allows more efficient memory usage in case of very noisy files because it does not have to
 * store characters while reading an element. Because of that, std::string_views can be used to store key value pairs.
 *
 * In the end, the unescaped key value pair views are converted into escaped key value pairs. At this stage, memory is allocated
 * to store characters, but noise is no longer an issue.
 * */
template <typename Response>
class LazyEscapingKeyValuePairExtractor : public KeyValuePairExtractor<Response>
{
public:
    LazyEscapingKeyValuePairExtractor(
        KeyStateHandler key_state_handler_,
        ValueStateHandler value_state_handler_,
        std::shared_ptr<KeyValuePairEscapingProcessor<Response>> escaping_processor_)
        : key_state_handler(key_state_handler_), value_state_handler(value_state_handler_), escaping_processor(escaping_processor_)
    {
    }

    [[nodiscard]] Response extract(const std::string & file) override
    {
        auto view = std::string_view {file};
        return extract(view);
    }

    [[nodiscard]] Response extract(std::string_view file) override
    {
        std::unordered_map<std::string_view, std::string_view> response_views;

        auto state = State::WAITING_KEY;

        std::size_t pos = 0;

        std::string_view key;
        std::string_view value;

        while (state != State::END)
        {
            auto next_state = processState(file, pos, state, key, value, response_views);

            pos = next_state.position_in_string;
            state = next_state.state;
        }

        return escaping_processor->process(response_views);
    }

private:
    NextState processState(std::string_view file, std::size_t pos, State state,
                           std::string_view & key, std::string_view & value,
                           std::unordered_map<std::string_view, std::string_view> & response_views)
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
                return flushPair(file, pos, key, value, response_views);
            case END:
                return {pos, state};
        }
    }

    NextState flushPair(const std::string_view & file, std::size_t pos, std::string_view key,
                        std::string_view value, std::unordered_map<std::string_view, std::string_view> & response_views)
    {
        response_views[key] = value;

        return {pos, pos == file.size() ? State::END : State::WAITING_KEY};
    }

    KeyStateHandler key_state_handler;
    ValueStateHandler value_state_handler;
    std::shared_ptr<KeyValuePairEscapingProcessor<Response>> escaping_processor;
};

}
