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
class LazyEscapingKeyValuePairExtractor : public KeyValuePairExtractor<Response> {
public:
    LazyEscapingKeyValuePairExtractor(KeyStateHandler key_state_handler_, ValueStateHandler value_state_handler_,
                                      std::shared_ptr<KeyValuePairEscapingProcessor<Response>> escaping_processor_)
    : key_state_handler(key_state_handler_), value_state_handler(value_state_handler_), escaping_processor(escaping_processor_){}

    [[nodiscard]] Response extract(const std::string & file) override
    {
        auto state = State::WAITING_KEY;

        std::size_t pos = 0;

        while (state != State::END) {
            auto nextState = processState(file, pos, state);

            pos = nextState.pos;
            state = nextState.state;
        }

        return escaping_processor->process(response_views);
    }

private:
    NextState processState(const std::string & file, std::size_t pos, State state)
    {
        switch (state) {
            case State::WAITING_KEY:
                return key_state_handler.wait(file, pos);
            case State::READING_KEY:
                return key_state_handler.read(file, pos);
            case State::READING_ENCLOSED_KEY:
                return key_state_handler.readEnclosed(file, pos);
            case State::READING_KV_DELIMITER:
                return key_state_handler.readKeyValueDelimiter(file, pos);
            case State::WAITING_VALUE:
                return value_state_handler.wait(file, pos);
            case State::READING_VALUE:
                return value_state_handler.read(file, pos);
            case State::READING_ENCLOSED_VALUE:
                return value_state_handler.readEnclosed(file, pos);
            case State::READING_EMPTY_VALUE:
                return value_state_handler.readEmpty(file, pos);
            case State::FLUSH_PAIR:
                return flushPair(file, pos);
            case END:
                return {
                    pos,
                    state
                };
        }
    }

    NextState flushPair(const std::string & file, std::size_t pos)
    {
        response_views[key_state_handler.get()] = value_state_handler.get();

        return {
            pos,
            pos == file.size() ? State::END : State::WAITING_KEY
        };
    }

    KeyStateHandler key_state_handler;
    ValueStateHandler value_state_handler;
    std::shared_ptr<KeyValuePairEscapingProcessor<Response>> escaping_processor;

    std::unordered_map<std::string_view, std::string_view> response_views;
};

}

