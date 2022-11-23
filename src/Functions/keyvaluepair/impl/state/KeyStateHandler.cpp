#include "KeyStateHandler.h"

namespace DB
{

KeyStateHandler::KeyStateHandler(char key_value_delimiter_, char escape_character_,
                                 std::optional<char> enclosing_character_)
    : StateHandler(escape_character_, enclosing_character_), key_value_delimiter(key_value_delimiter_)
{}

NextState KeyStateHandler::waitKey(const std::string &file, size_t pos) const {
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

NextStateWithElement KeyStateHandler::readKey(const std::string &file, size_t pos) const {
    bool escape = false;

    auto start_index = pos;

    while (pos < file.size()) {
        const auto current_character = file[pos++];
        if (escape) {
            escape = false;
        } else if (escape_character == current_character) {
            escape = true;
        } else if (current_character == key_value_delimiter) {
            // not checking for empty key because with current waitKey implementation
            // there is no way this piece of code will be reached for the very first key character
            return {
                {
                    pos,
                    State::WAITING_VALUE
                },
                createElement(file, start_index, pos - 1)
            };
        } else if (!std::isalnum(current_character) && current_character != '_') {
            return {
                {
                    pos,
                    State::WAITING_KEY
                },
                {}
            };
        }
    }

    return {
        {
            pos,
            State::END
        },
        {}
    };
}

NextStateWithElement KeyStateHandler::readEnclosedKey(const std::string &file, size_t pos) const {
    auto start_index = pos;

    while (pos < file.size()) {
        const auto current_character = file[pos++];

        if (enclosing_character == current_character) {
            auto is_key_empty = start_index == pos;

            if (is_key_empty) {
                return {
                    {
                        pos,
                        State::WAITING_KEY
                    },
                    {}
                };
            }

            return {
                {
                    pos,
                    State::READING_KV_DELIMITER
                },
                createElement(file, start_index, pos - 1)
            };
        }
    }

    return {
        {
            pos,
            State::END
        },
        {}
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

}
