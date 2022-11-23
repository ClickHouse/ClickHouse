#include "ValueStateHandler.h"

namespace DB
{

ValueStateHandler::ValueStateHandler(char escape_character_, char item_delimiter_,
                                     std::optional<char> enclosing_character_,
                                     std::unordered_set<char> special_character_allowlist_)
    : StateHandler(escape_character_, enclosing_character_), item_delimiter(item_delimiter_),
    special_character_allowlist(std::move(special_character_allowlist_))
{}

NextState ValueStateHandler::waitValue(const std::string &file, size_t pos) const {
    while (pos < file.size()) {
        const auto current_character = file[pos];

        if (current_character == enclosing_character) {
            return {
                pos + 1u,
                State::READING_ENCLOSED_VALUE
            };
        } else if (current_character == item_delimiter) {
            return {
                pos,
                State::READING_EMPTY_VALUE
            };
        } else if (isValidCharacter(current_character)) {
            return {
                pos,
                State::READING_VALUE
            };
        } else {
            pos++;
        }
    }

    return {
        pos,
        State::READING_EMPTY_VALUE
    };
}

NextStateWithElement ValueStateHandler::readValue(const std::string &file, size_t pos) const {
    bool escape = false;

    auto start_index = pos;

    while (pos < file.size()) {
        const auto current_character = file[pos++];
        if (escape) {
            escape = false;
        } else if (escape_character == current_character) {
            escape = true;
        } else if (current_character == item_delimiter || !isValidCharacter(current_character)) {
            return {
                {
                    pos,
                    State::FLUSH_PAIR
                },
                createElement(file, start_index, pos - 1)
            };
        }
    }

    // TODO: do I really need the below logic?
    // this allows empty values at the end
    return {
        {
            pos,
            State::FLUSH_PAIR
        },
        createElement(file, start_index, pos)
    };
}

NextStateWithElement ValueStateHandler::readEnclosedValue(const std::string &file, size_t pos) const {
    auto start_index = pos;

    while (pos < file.size()) {
        const auto current_character = file[pos++];
        if (enclosing_character == current_character) {
            // not checking for empty value because with current waitValue implementation
            // there is no way this piece of code will be reached for the very first value character
            return {
                {
                    pos,
                    State::FLUSH_PAIR
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

NextStateWithElement ValueStateHandler::readEmptyValue(const std::string &, size_t pos) const {
    return {
        {
            pos + 1,
            State::FLUSH_PAIR
        },
        {}
    };
}

bool ValueStateHandler::isValidCharacter(char character) const {
    return special_character_allowlist.contains(character) || std::isalnum(character) || character == '_';
}

}
