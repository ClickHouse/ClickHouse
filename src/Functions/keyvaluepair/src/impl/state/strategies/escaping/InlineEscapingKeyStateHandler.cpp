#include "InlineEscapingKeyStateHandler.h"
#include <Functions/keyvaluepair/src/impl/state/strategies/util/CharacterFinder.h>
#include <Functions/keyvaluepair/src/impl/state/strategies/util/EscapedCharacterReader.h>
#include <Functions/keyvaluepair/src/impl/state/strategies/util/NeedleFactory.h>

namespace DB
{

InlineEscapingKeyStateHandler::InlineEscapingKeyStateHandler(Configuration configuration_)
    : extractor_configuration(std::move(configuration_))
{
    wait_needles = EscapingNeedleFactory::getWaitNeedles(extractor_configuration);
    read_needles = EscapingNeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = EscapingNeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState InlineEscapingKeyStateHandler::wait(std::string_view file, size_t pos) const
{
    BoundsSafeCharacterFinder finder;

    const auto quoting_character = extractor_configuration.quoting_character;

    while (auto character_position_opt = finder.findFirstNot(file, pos, wait_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];

        if (quoting_character == character)
        {
            return {character_position + 1u, State::READING_QUOTED_KEY};
        }
        else
        {
            return {character_position, State::READING_KEY};
        }
    }

    return {file.size(), State::END};
}

/*
 * I only need to iteratively copy stuff if there are escape sequences. If not, views are sufficient.
 * TSKV has a nice catch for that, implementers kept an auxiliary string to hold copied characters.
 * If I find a key value delimiter and that is empty, I do not need to copy? hm,m hm hm
 * */

NextState InlineEscapingKeyStateHandler::read(std::string_view file, size_t pos, ElementType & key) const
{
    BoundsSafeCharacterFinder finder;

    const auto & [key_value_delimiter, quoting_character, pair_delimiters]
        = extractor_configuration;

    key.clear();

    /*
     * Maybe modify finder return type to be the actual pos. In case of failures, it shall return pointer to the end.
     * It might help updating current pos?
     * */

    while (auto character_position_opt = finder.findFirst(file, pos, read_needles))
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
            next_pos = next_byte_ptr - file.begin();

            if (escaped_characters.empty())
            {
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
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), character) != pair_delimiters.end())
        {
            return {next_pos, State::WAITING_KEY};
        }

        pos = next_pos;
    }

    // might be problematic in case string reaches the end and I haven't copied anything over to key

    return {file.size(), State::END};
}

NextState InlineEscapingKeyStateHandler::readQuoted(std::string_view file, size_t pos, ElementType & key) const
{
    BoundsSafeCharacterFinder finder;

    const auto quoting_character = extractor_configuration.quoting_character;

    key.clear();

    while (auto character_position_opt = finder.findFirst(file, pos, read_quoted_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (character == EscapedCharacterReader::ESCAPE_CHARACTER)
        {
            for (auto i = pos; i < character_position; i++)
            {
                key.push_back(file[i]);
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
                    key.push_back(escaped_character);
                }
            }
        }
        else if (quoting_character == character)
        {
            // todo try to optimize with resize and memcpy
            for (auto i = pos; i < character_position; i++)
            {
                key.push_back(file[i]);
            }

            if (key.empty())
            {
                return {next_pos, State::WAITING_KEY};
            }

            return {next_pos, State::READING_KV_DELIMITER};
        }

        pos = next_pos;
    }

    return {file.size(), State::END};
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
        return {pos, extractor_configuration.key_value_delimiter == current_character ? State::WAITING_VALUE : State::WAITING_KEY};
    }
}

}
