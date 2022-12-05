#include "KeyStateHandler.h"

namespace DB
{

KeyStateHandler::KeyStateHandler(char key_value_delimiter_, char escape_character_,
                                 std::optional<char> enclosing_character_)
    : StateHandler(escape_character_, enclosing_character_), key_value_delimiter(key_value_delimiter_)
{}

NextState KeyStateHandler::wait(const std::string &file, size_t pos) const {
    while (pos < file.size()) {
        const auto current_character = file[pos];
        if (isalpha(current_character)) {
            return {
                pos,
                State::READING_KEY
            };
        } else if (enclosing_character && current_character == enclosing_character) {
            return {
                pos + 1u,
                State::READING_ENCLOSED_KEY
            };
        } else {
            pos++;
        }
    }

    return {
        pos,
        State::END
    };
}

NextState KeyStateHandler::read(const std::string &file, size_t pos) {
    bool escape = false;

    auto start_index = pos;

    key = {};

    while (pos < file.size()) {
        const auto current_character = file[pos++];
        if (escape) {
            escape = false;
        } else if (escape_character == current_character) {
            escape = true;
        } else if (current_character == key_value_delimiter) {
            // not checking for empty key because with current waitKey implementation
            // there is no way this piece of code will be reached for the very first key character
            key = createElement(file, start_index, pos - 1);
            return {
                pos,
                State::WAITING_VALUE
            };
        } else if (!std::isalnum(current_character) && current_character != '_') {
            return {
                pos,
                State::WAITING_KEY
            };
        }
    }

    return {
        pos,
        State::END
    };
}

NextState KeyStateHandler::readEnclosed(const std::string &file, size_t pos) {
    auto start_index = pos;
    key = {};

    while (pos < file.size()) {
        const auto current_character = file[pos++];

        if (enclosing_character == current_character) {
            auto is_key_empty = start_index == pos;

            if (is_key_empty) {
                return {
                    pos,
                    State::WAITING_KEY
                };
            }

            key = createElement(file, start_index, pos - 1);
            return {
                pos,
                State::READING_KV_DELIMITER
            };
        }
    }

    return {
        pos,
        State::END
    };
}

NextState KeyStateHandler::readKeyValueDelimiter(const std::string &file, size_t pos) const {
    if (pos == file.size()) {
        return {
            pos,
            State::END
        };
    } else {
        const auto current_character = file[pos++];
        return {
            pos,
            current_character == key_value_delimiter ? State::WAITING_VALUE : State::WAITING_KEY
        };
    }
}

std::string_view KeyStateHandler::get() const {
    return key;
}

}
