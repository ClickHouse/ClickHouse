#pragma once

#include <string>
#include "State.h"
#include "StateHandler.h"
#include <unordered_set>

namespace DB
{

class InlineEscapingValueStateHandler : public StateHandler
{
public:
    using ElementType = std::string;

    InlineEscapingValueStateHandler(
        char escape_character_,
        char item_delimiter_,
        std::optional<char> enclosing_character_,
        std::unordered_set<char> special_character_allowlist_)
        : StateHandler(enclosing_character_), escape_character(escape_character_)
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
        bool escape = false;

        value.clear();

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if (escape)
            {
                escape = false;
                value.push_back(current_character);
                continue;
            }
            else if (escape_character == current_character)
            {
                escape = true;
                continue;
            }
            if (current_character == item_delimiter || !isValidCharacter(current_character))
            {
                return {pos, State::FLUSH_PAIR};
            }
            else
            {
                value.push_back(current_character);
            }
        }

        return {pos, State::FLUSH_PAIR};
    }

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & value)
    {
        value.clear();

        while (pos < file.size())
        {
            const auto current_character = file[pos++];
            if (enclosing_character == current_character)
            {
                return {pos, State::FLUSH_PAIR};
            }
            else
            {
                value.push_back(current_character);
            }
        }

        return {pos, State::END};
    }

    [[nodiscard]] static NextState readEmpty(std::string_view, size_t pos, ElementType & value)
    {
        value.clear();
        return {pos + 1, State::FLUSH_PAIR};
    }

private:
    const char escape_character;
    const char item_delimiter;
    std::unordered_set<char> special_character_allowlist;

    bool isValidCharacter(char character) const
    {
        return std::isalnum(character) || character == '_' || special_character_allowlist.contains(character);
    }
};

}
