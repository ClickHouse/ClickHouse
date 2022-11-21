#include "LazyEscapingKeyValuePairExtractor.h"
#include "KeyValuePairEscapingProcessor.h"

#include <optional>

LazyEscapingKeyValuePairExtractor::LazyEscapingKeyValuePairExtractor(char item_delimiter, char key_value_delimiter, char escape_character_, std::optional<char> enclosing_character)
    : escape_character(escape_character_), keyStateHandler(key_value_delimiter, escape_character, enclosing_character),
    valueStateHandler(escape_character, item_delimiter, enclosing_character)
{}

LazyEscapingKeyValuePairExtractor::Response LazyEscapingKeyValuePairExtractor::extract(const std::string & file) {

    auto state = State::WAITING_KEY;

    std::size_t pos = 0;

    while (state != State::END) {
        auto nextState = extract(file, pos, state);

        pos = nextState.pos;
        state = nextState.state;
    }

    KeyValuePairEscapingProcessor escapingProcessor(escape_character);

    return escapingProcessor.process(response_views);
}

NextState LazyEscapingKeyValuePairExtractor::extract(const std::string & file, std::size_t pos, State state) {
    switch (state) {
        case State::WAITING_KEY:
            return waitKey(file, pos);
        case State::READING_KEY:
            return readKey(file, pos);
        case State::READING_ENCLOSED_KEY:
            return readEnclosedKey(file, pos);
        case State::READING_KV_DELIMITER:
            return readKeyValueDelimiter(file, pos);
        case State::WAITING_VALUE:
            return waitValue(file, pos);
        case State::READING_VALUE:
            return readValue(file, pos);
        case State::READING_ENCLOSED_VALUE:
            return readEnclosedValue(file, pos);
        case State::READING_EMPTY_VALUE:
            return readEmptyValue(file, pos);
        case State::FLUSH_PAIR:
            return flushPair(file, pos);
        case END:
            return {
                pos,
                state
            };
    }
}


NextState LazyEscapingKeyValuePairExtractor::waitKey(const std::string & file, size_t pos) const {
    return keyStateHandler.waitKey(file, pos);
}

NextState LazyEscapingKeyValuePairExtractor::readKeyValueDelimiter(const std::string &file, size_t pos) const {
    return keyStateHandler.readKeyValueDelimiter(file, pos);
}

NextState LazyEscapingKeyValuePairExtractor::readKey(const std::string & file, size_t pos) {
    auto [next_state, next_key] = keyStateHandler.readKey(file, pos);

    key = next_key;

    return next_state;
}

NextState LazyEscapingKeyValuePairExtractor::readEnclosedKey(const std::string &file, size_t pos) {
    auto [next_state, next_key] = keyStateHandler.readEnclosedKey(file, pos);

    key = next_key;

    return next_state;
}

NextState LazyEscapingKeyValuePairExtractor::waitValue(const std::string &file, size_t pos) const {
    return valueStateHandler.waitValue(file, pos);
}

NextState LazyEscapingKeyValuePairExtractor::readValue(const std::string &file, size_t pos) {
    auto [next_state, next_value] = valueStateHandler.readValue(file, pos);

    value = next_value;

    return next_state;
}

NextState LazyEscapingKeyValuePairExtractor::readEnclosedValue(const std::string &file, size_t pos) {
    auto [next_state, next_value] = valueStateHandler.readEnclosedValue(file, pos);

    value = next_value;

    return next_state;
}

NextState LazyEscapingKeyValuePairExtractor::readEmptyValue(const std::string &file, size_t pos) {
    auto [next_state, next_value] = valueStateHandler.readEmptyValue(file, pos);

    value = next_value;

    return next_state;
}

NextState LazyEscapingKeyValuePairExtractor::flushPair(const std::string &file, std::size_t pos) {
    response_views[key] = value;

    return {
        pos,
        pos == file.size() ? State::END : State::WAITING_KEY
    };
}
