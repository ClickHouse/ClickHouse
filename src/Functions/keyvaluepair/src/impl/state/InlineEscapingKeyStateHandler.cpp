#include "InlineEscapingKeyStateHandler.h"

namespace DB
{

InlineEscapingKeyStateHandler::InlineEscapingKeyStateHandler(char key_value_delimiter_, char escape_character_, std::optional<char> enclosing_character_)
    : StateHandler(enclosing_character_), escape_character(escape_character_), key_value_delimiter(key_value_delimiter_)
{}

NextState InlineEscapingKeyStateHandler::wait(std::string_view file, size_t pos) const
{
    while (pos < file.size())
    {
        const auto current_character = file[pos];

        if (current_character == escape_character)
        {
            return {pos, State::READING_KEY};
        }
        else if (isValidCharacter(current_character))
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

NextState InlineEscapingKeyStateHandler::read(std::string_view file, size_t pos, ElementType & key) const
{
    bool escape = false;

    key.clear();

    while (pos < file.size())
    {
        const auto current_character = file[pos++];

        if (escape)
        {
            key.push_back(current_character);
            escape = false;
            continue;
        }
        else if (escape_character == current_character)
        {
            escape = true;
            continue;
        }
        else if (current_character == key_value_delimiter)
        {
            return {pos, State::WAITING_VALUE};
        }
        else if (!isValidCharacter(current_character))
        {
            return {pos, State::WAITING_KEY};
        }
        else
        {
            key.push_back(current_character);
        }
    }

    return {pos, State::END};
}

NextState InlineEscapingKeyStateHandler::readEnclosed(std::string_view file, size_t pos, ElementType & key) const
{
    key.clear();

    while (pos < file.size())
    {
        const auto current_character = file[pos++];

        if (*enclosing_character == current_character)
        {
            if (key.empty())
            {
                return {pos, State::WAITING_KEY};
            }

            return {pos, State::READING_KV_DELIMITER};
        }
        else
        {
            key.push_back(current_character);
        }
    }

    return {pos, State::END};
}

NextState InlineEscapingKeyStateHandler::readKeyValueDelimiter(std::string_view file, size_t pos) const
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

bool InlineEscapingKeyStateHandler::isValidCharacter(char character)
{
    return std::isalnum(character) || character == '_';
}

}
