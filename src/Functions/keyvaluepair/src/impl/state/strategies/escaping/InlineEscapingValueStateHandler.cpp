#include "InlineEscapingValueStateHandler.h"
#include "Functions/keyvaluepair/src/impl/state/util/EscapedCharacterReader.h"
#include "Functions/keyvaluepair/src/impl/state/util/CharacterFinder.h"

namespace DB
{

InlineEscapingValueStateHandler::InlineEscapingValueStateHandler(ExtractorConfiguration extractor_configuration_)
    : extractor_configuration(std::move(extractor_configuration_))
{
}

NextState InlineEscapingValueStateHandler::wait(std::string_view file, size_t pos) const
{
    const auto & [key_value_delimiter, pair_delimiters, quoting_characters]
        = extractor_configuration;

    while (pos < file.size())
    {
        const auto current_character = file[pos];

        if (quoting_characters.contains(current_character))
        {
            return {pos + 1u, State::READING_ENCLOSED_VALUE};
        }
        else if (pair_delimiters.contains(current_character))
        {
            return {pos, State::READING_EMPTY_VALUE};
        }
        else if (key_value_delimiter == current_character)
        {
            return {pos, State::WAITING_KEY};
        }
        // skip leading white spaces, re-think this
        else if (current_character != ' ')
        {
            return {pos, State::READING_VALUE};
        }

        pos++;
    }

    return {pos, State::READING_EMPTY_VALUE};
}

NextState InlineEscapingValueStateHandler::read(std::string_view file, size_t pos, ElementType & value) const
{
    CharacterFinder finder;

    const auto & [key_value_delimiter, pair_delimiters, quoting_characters]
        = extractor_configuration;

    std::vector<char> needles;

    needles.push_back(EscapedCharacterReader::ESCAPE_CHARACTER);
    needles.push_back(key_value_delimiter);

    std::copy(quoting_characters.begin(), quoting_characters.end(), std::back_inserter(needles));
    std::copy(pair_delimiters.begin(), pair_delimiters.end(), std::back_inserter(needles));

    value.clear();

    /*
     * Maybe modify finder return type to be the actual pos. In case of failures, it shall return pointer to the end.
     * It might help updating current pos?
     * */

    while (auto character_position_opt = finder.find_first(file, pos, needles))
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
            // fix later, -1 looks uglyyyyy
            next_pos = next_byte_ptr - file.begin();

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
                    value.push_back(escaped_character);
                }
            }
        }
        else if (key_value_delimiter == character)
        {
            return {next_pos, State::WAITING_KEY};
        }
        // pair delimiter
        else if (pair_delimiters.contains(character))
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
    CharacterFinder finder;

    const auto & quoting_characters = extractor_configuration.quoting_characters;

    std::vector<char> needles;

    needles.push_back(EscapedCharacterReader::ESCAPE_CHARACTER);

    std::copy(quoting_characters.begin(), quoting_characters.end(), std::back_inserter(needles));

    value.clear();

    while (auto character_position_opt = finder.find_first(file, pos, needles))
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
            // fix later, -1 looks uglyyyyy
            next_pos = next_byte_ptr - file.begin();

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
                    value.push_back(escaped_character);
                }
            }
        }
        else if(quoting_characters.contains(character))
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
