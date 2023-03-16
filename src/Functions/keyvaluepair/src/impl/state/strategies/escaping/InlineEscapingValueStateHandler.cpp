#include "InlineEscapingValueStateHandler.h"
#include <Functions/keyvaluepair/src/impl/state/strategies/util/CharacterFinder.h>
#include <Functions/keyvaluepair/src/impl/state/strategies/util/EscapedCharacterReader.h>
#include <Functions/keyvaluepair/src/impl/state/strategies/util/NeedleFactory.h>

namespace DB
{

InlineEscapingValueStateHandler::InlineEscapingValueStateHandler(Configuration extractor_configuration_)
    : extractor_configuration(std::move(extractor_configuration_))
{
    read_needles = EscapingNeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = EscapingNeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState InlineEscapingValueStateHandler::wait(std::string_view file, size_t pos) const
{
    const auto & [key_value_delimiter, quoting_character, pair_delimiters]
        = extractor_configuration;

    if (pos < file.size())
    {
        const auto current_character = file[pos];

        if (quoting_character == current_character)
        {
            return {pos + 1u, State::READING_ENCLOSED_VALUE};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), current_character) != pair_delimiters.end())
        {
            return {pos, State::READING_EMPTY_VALUE};
        }
        else if (key_value_delimiter == current_character)
        {
            return {pos, State::WAITING_KEY};
        }
        else
        {
            return {pos, State::READING_VALUE};
        }
    }

    return {pos, State::READING_EMPTY_VALUE};
}

NextState InlineEscapingValueStateHandler::read(std::string_view file, size_t pos, ElementType & value) const
{
    BoundsSafeCharacterFinder finder;

    const auto & [key_value_delimiter, quoting_character, pair_delimiters]
        = extractor_configuration;

    value.clear();

    /*
     * Maybe modify finder return type to be the actual pos. In case of failures, it shall return pointer to the end.
     * It might help updating current pos?
     * */

    while (auto character_position_opt = finder.find_first(file, pos, read_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (EscapedCharacterReader::isEscapeCharacter(character))
        {
            for (auto i = pos; i < character_position; i++)
            {
                value.push_back(file[i]);
            }

            auto [next_byte_ptr, escaped_characters] = EscapedCharacterReader::read(file, character_position);
            next_pos = next_byte_ptr - file.begin();

            if (escaped_characters.empty())
            {
                return {next_pos, State::WAITING_KEY};
            }
            else
            {
                for (auto escaped_character : escaped_characters)
                {
                    value.push_back(escaped_character);
                }
            }
        }
        else if (key_value_delimiter == character)
        {
            return {next_pos, State::WAITING_KEY};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), character) != pair_delimiters.end())
        {
            // todo try to optimize with resize and memcpy
            for (auto i = pos; i < character_position; i++)
            {
                value.push_back(file[i]);
            }

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    for (; pos < file.size(); pos++)
    {
        value.push_back(file[pos]);
    }

    return {pos, State::FLUSH_PAIR};
}

NextState InlineEscapingValueStateHandler::readEnclosed(std::string_view file, size_t pos, ElementType & value) const
{
    BoundsSafeCharacterFinder finder;

    const auto quoting_character = extractor_configuration.quoting_character;

    value.clear();

    while (auto character_position_opt = finder.find_first(file, pos, read_quoted_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (character == EscapedCharacterReader::ESCAPE_CHARACTER)
        {
            for (auto i = pos; i < character_position; i++)
            {
                value.push_back(file[i]);
            }

            auto [next_byte_ptr, escaped_characters] = EscapedCharacterReader::read(file, character_position);
            next_pos = next_byte_ptr - file.begin();

            if (escaped_characters.empty())
            {
                return {next_pos, State::WAITING_KEY};
            }
            else
            {
                for (auto escaped_character : escaped_characters)
                {
                    value.push_back(escaped_character);
                }
            }
        }
        else if(quoting_character == character)
        {
            // todo try to optimize with resize and memcpy
            for (auto i = pos; i < character_position; i++)
            {
                value.push_back(file[i]);
            }

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    return {pos, State::END};
}

NextState InlineEscapingValueStateHandler::readEmpty(std::string_view, size_t pos, ElementType & value)
{
    value.clear();
    return {pos + 1, State::FLUSH_PAIR};
}

}
