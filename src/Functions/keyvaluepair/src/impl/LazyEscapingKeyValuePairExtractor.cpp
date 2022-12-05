#include <optional>
#include "LazyEscapingKeyValuePairExtractor.h"

namespace DB
{

LazyEscapingKeyValuePairExtractor::LazyEscapingKeyValuePairExtractor(KeyStateHandler key_state_handler_,
                                                                     ValueStateHandler value_state_handler_,
                                                                     KeyValuePairEscapingProcessor escaping_processor_)
    : key_state_handler(key_state_handler_), value_state_handler(value_state_handler_), escaping_processor(escaping_processor_)
{}

LazyEscapingKeyValuePairExtractor::Response LazyEscapingKeyValuePairExtractor::extract(const std::string & file) {

    auto state = State::WAITING_KEY;

    std::size_t pos = 0;

    while (state != State::END) {
        auto nextState = extract(file, pos, state);

        pos = nextState.pos;
        state = nextState.state;
    }

    return escaping_processor.process(response_views);
}

NextState LazyEscapingKeyValuePairExtractor::extract(const std::string & file, std::size_t pos, State state) {
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

NextState LazyEscapingKeyValuePairExtractor::flushPair(const std::string & file, std::size_t pos) {
    response_views[key_state_handler.get()] = value_state_handler.get();

    return {
        pos,
        pos == file.size() ? State::END : State::WAITING_KEY
    };
}

}
