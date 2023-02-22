#pragma once

#include "StateHandler.h"

namespace DB
{

class NoEscapingKeyStateHandler : public StateHandler
{
public:
    using ElementType = std::string_view;

    NoEscapingKeyStateHandler(char key_value_delimiter_, std::optional<char> enclosing_character_)
        : StateHandler(enclosing_character_), key_value_delimiter(key_value_delimiter_)
    {
    }

    [[nodiscard]] NextState wait(std::string_view file, size_t pos)
    {
        while (pos < file.size())
        {
            const auto current_character = file[pos];

            if (isValidCharacter(current_character))
            {
                return {pos, State::READING_KEY};
            }
            else if (enclosing_character && current_character == enclosing_character)
            {
                return {pos + 1u, State::READING_ENCLOSED_KEY};
            }

            pos++;
        }

        return {pos, State::END};
    }

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const
    {
        auto start_index = pos;

        key = {};

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if (current_character == key_value_delimiter)
            {
                // not checking for empty key because with current waitKey implementation
                // there is no way this piece of code will be reached for the very first key character
                key = createElement(file, start_index, pos - 1);
                return {pos, State::WAITING_VALUE};
            }
            else if (!isValidCharacter(current_character))
            {
                return {pos, State::WAITING_KEY};
            }
        }

        return {pos, State::END};
    }

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & key)
    {
        auto start_index = pos;

        key = {};

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if (enclosing_character == current_character)
            {
                auto is_key_empty = start_index == pos;

                if (is_key_empty)
                {
                    return {pos, State::WAITING_KEY};
                }

                key = createElement(file, start_index, pos - 1);
                return {pos, State::READING_KV_DELIMITER};
            }
        }

        return {pos, State::END};
    }

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const
    {
        if (pos == file.size())
        {
            return {pos, State::END};
        }
        else
        {
            const auto current_character = file[pos++];
            return {pos, current_character == key_value_delimiter ? State::WAITING_VALUE : State::WAITING_KEY};
        }
    }

private:
    const char key_value_delimiter;

    static bool isValidCharacter(char character)
    {
        return std::isalnum(character) || character == '_';
    }
};

}
