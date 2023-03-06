#include "InlineEscapingKeyStateHandler.h"
#include "util/EscapedCharacterReader.h"
#include "util/CharacterFinder.h"
#include <unordered_set>

namespace DB
{

InlineEscapingKeyStateHandler::InlineEscapingKeyStateHandler(char key_value_delimiter_, std::optional<char> enclosing_character_)
    : StateHandler(enclosing_character_), key_value_delimiter(key_value_delimiter_)
{}

NextState InlineEscapingKeyStateHandler::wait(std::string_view file, size_t pos) const
{
    // maybe wait should be - find first non control character

    static constexpr auto is_special = [](char character) {
        return character == '\\' || character == '=';
    };

    std::vector<char> special_characters;
    special_characters.push_back(EscapedCharacterReader::ESCAPE_CHARACTER);

//    CharacterFinder finder;
//
//    while (auto character_position_opt = finder.find_first_not(file, pos, special_characters))
//    {
//
//    }

    while (pos < file.size())
    {
        const auto current_character = file[pos];

        if (current_character == enclosing_character)
        {
            return {pos, State::READING_ENCLOSED_KEY};
        }
        else if (!is_special(current_character))
        {
            return {pos, State::READING_KEY};
        }

        pos++;
    }

    return {pos, State::END};
}

/*
 * I only need to iteratively copy stuff if there are escape sequences. If not, views are sufficient.
 * TSKV has a nice catch for that, implementers kept an auxiliary string to hold copied characters.
 * If I find a key value delimiter and that is empty, I do not need to copy? hm,m hm hm
 * */

NextState InlineEscapingKeyStateHandler::read(std::string_view file, size_t pos, ElementType & key) const
{
    CharacterFinder finder;

    key.clear();

    while (auto character_position_opt = finder.find_first(file, pos, {'\\', '"', '='}))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (EscapedCharacterReader::isEscapeCharacter(character))
        {
            for (auto i = pos; i < character_position; i++)
            {
                key.push_back(file[i]);
            }

            auto [next_byte_ptr, escaped_characters] = EscapedCharacterReader::read(file, character_position);
            // fix later, -1 looks uglyyyyy
            next_pos += (next_byte_ptr - file.begin() - 1u);

            if (escaped_characters.empty())
            {
                // I have to consider a case like the following: "name:arthur\".
                // will it be discarded or not?
                // current implementation will discard it.
                return {next_pos, State::WAITING_KEY};
            }
            else
            {
                for (auto escaped_character : escaped_characters)
                {
                    key.push_back(escaped_character);
                }
            }
        }
        else if (character == key_value_delimiter)
        {
            // todo try to optimize with resize and memcpy
            for (auto i = pos; i < character_position; i++)
            {
                key.push_back(file[i]);
            }

            return {next_pos, State::WAITING_VALUE};
        }
        // pair delimiter
        else if (character == ',')
        {
            return {next_pos, State::WAITING_KEY};
        }

        pos = next_pos;
    }

    // might be problematic in case string reaches the end and I haven't copied anything over to key

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
