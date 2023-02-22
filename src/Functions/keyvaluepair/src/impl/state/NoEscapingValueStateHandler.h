#pragma once

#include "StateHandler.h"
#include <unordered_set>

namespace DB
{

class NoEscapingValueStateHandler : public StateHandler
{
public:
    using ElementType = std::string_view;

    NoEscapingValueStateHandler(
        char item_delimiter_,
        std::optional<char> enclosing_character_,
        std::unordered_set<char> special_character_allowlist_)
        : StateHandler(enclosing_character_)
        , item_delimiter(item_delimiter_), special_character_allowlist(special_character_allowlist_)
    {
    }

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const
    {
        while (pos < file.size())
        {
            const auto current_character = file[pos];

            if (enclosing_character && current_character == enclosing_character)
            {
                return {pos + 1u, State::READING_ENCLOSED_VALUE};
            }
            else if (current_character == item_delimiter)
            {
                return {pos, State::READING_EMPTY_VALUE};
            }
            else if (isValidCharacter(current_character))
            {
                return {pos, State::READING_VALUE};
            }
            else
            {
                pos++;
            }
        }

        return {pos, State::READING_EMPTY_VALUE};
    }

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & value)
    {
        auto start_index = pos;

        value = {};

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if (current_character == item_delimiter || !isValidCharacter(current_character))
            {
                value = createElement(file, start_index, pos - 1);
                return {pos, State::FLUSH_PAIR};
            }
        }

        // TODO: do I really need the below logic?
        // this allows empty values at the end
        value = createElement(file, start_index, pos);
        return {pos, State::FLUSH_PAIR};
    }

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & value)
    {
        auto start_index = pos;

        value = {};

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if (enclosing_character == current_character)
            {
                // not checking for empty value because with current waitValue implementation
                // there is no way this piece of code will be reached for the very first value character
                value = createElement(file, start_index, pos - 1);
                return {pos, State::FLUSH_PAIR};
            }
        }

        return {pos, State::END};
    }

    [[nodiscard]] static NextState readEmpty(std::string_view, size_t pos, ElementType & value)
    {
        value = {};
        return {pos + 1, State::FLUSH_PAIR};
    }

private:
    const char item_delimiter;
    std::unordered_set<char> special_character_allowlist;

    bool isValidCharacter(char character) const
    {
        return std::isalnum(character) || character == '_' || special_character_allowlist.contains(character);
    }
};

}
