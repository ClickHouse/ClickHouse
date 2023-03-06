#include "InlineEscapingValueStateHandler.h"
#include "util/EscapedCharacterReader.h"

namespace DB
{

InlineEscapingValueStateHandler::InlineEscapingValueStateHandler(
    char item_delimiter_,
    std::optional<char> enclosing_character_,
    std::unordered_set<char> special_character_allowlist_)
    : StateHandler(enclosing_character_)
    , item_delimiter(item_delimiter_), special_character_allowlist(special_character_allowlist_)
{
}

NextState InlineEscapingValueStateHandler::wait(std::string_view file, size_t pos) const
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

NextState InlineEscapingValueStateHandler::read(std::string_view file, size_t pos, ElementType & value) const
{
    bool escape = false;

    value.clear();

    while (pos < file.size())
    {
        const auto current_character = file[pos];
        const auto next_pos = pos + 1u;

        if (escape)
        {
            escape = false;

//            if (auto escaped_character = EscapedCharacterReader::read({file.begin() + pos, file.end()}))
//            {
//                value.push_back(*escaped_character);
//            }
//            else
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
        else if (current_character == item_delimiter || !isValidCharacter(current_character))
        {
            return {next_pos, State::FLUSH_PAIR};
        }
        else
        {
            value.push_back(current_character);
        }

        pos = next_pos;
    }

    return {pos, State::FLUSH_PAIR};
}

NextState InlineEscapingValueStateHandler::readEnclosed(std::string_view file, size_t pos, ElementType & value) const
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

NextState InlineEscapingValueStateHandler::readEmpty(std::string_view, size_t pos, ElementType & value)
{
    value.clear();
    return {pos + 1, State::FLUSH_PAIR};
}

bool InlineEscapingValueStateHandler::isValidCharacter(char character) const
{
    return std::isalnum(character) || character == '_' || special_character_allowlist.contains(character);
}

}
