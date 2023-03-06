#pragma once

#include "StateHandler.h"
#include <string>

namespace DB
{

enum class EscapingStrategy2
{
    WITH_ESCAPING,
    WITHOUT_ESCAPING
};

template <typename Strategy>
class MultiStrategyKeyStateHandler : public StateHandler
{
public:
    MultiStrategyKeyStateHandler(char key_value_delimiter_, std::optional<char> enclosing_character_)
        : StateHandler(enclosing_character_), key_value_delimiter(key_value_delimiter_) {}

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const
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

    [[nodiscard]] NextState read(std::string_view file, std::size_t pos, auto key) const
    {
        return static_cast<Strategy*>(this)->read(file, pos, key);
    }

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, auto & key) const
    {
        return static_cast<Strategy*>(this)->readEnclosed(file, pos, key);
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

protected:
    const char key_value_delimiter;

    static bool isValidCharacter(char character)
    {
        return std::isalnum(character) || character == '_';
    }
};

class EscapingStrategyStateHandler : public MultiStrategyKeyStateHandler<EscapingStrategyStateHandler>
{
    using ElementType = std::string;
public:
    EscapingStrategyStateHandler(char key_value_delimiter_, std::optional<char> enclosing_character_)
        : MultiStrategyKeyStateHandler(key_value_delimiter_, enclosing_character_) {}

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const
    {
        bool escape = false;

        key.clear();

        while (pos < file.size())
        {
            const auto current_character = file[pos];
            const auto next_pos = pos + 1u;

            if (escape)
            {
                escape = false;

//                if (auto escaped_character = EscapedCharacterReader::read({file.begin() + pos, file.end()}))
//                {
//                    key.push_back(*escaped_character);
//                }
//                else
                {
                    // Discard in case of failures. It can fail either on converting characters into a number (\xHEX \0OCTAL)
                    // or if there isn't enough characters left in the string
                    return {next_pos, State::WAITING_KEY };
                }
            }
            else if (EscapedCharacterReader::isEscapeCharacter(current_character))
            {
                escape = true;
            }
            else if (current_character == key_value_delimiter)
            {
                return {next_pos, State::WAITING_VALUE};
            }
            else if (!isValidCharacter(current_character))
            {
                return {next_pos, State::WAITING_KEY};
            }
            else
            {
                key.push_back(current_character);
            }

            pos = next_pos;
        }

        return {pos, State::END};
    }
};

}
