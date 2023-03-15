#include "NoEscapingKeyStateHandler.h"
#include <Functions/keyvaluepair/src/impl/state/strategies/util/CharacterFinder.h>
#include <Functions/keyvaluepair/src/impl/state/strategies/util/NeedleFactory.h>

namespace DB
{

NoEscapingKeyStateHandler::NoEscapingKeyStateHandler(ExtractorConfiguration extractor_configuration_)
: extractor_configuration(std::move(extractor_configuration_))
{
    wait_needles = NeedleFactory::getWaitNeedles(extractor_configuration);
    read_needles = NeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = NeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState NoEscapingKeyStateHandler::wait(std::string_view file, size_t pos) const
{
    BoundsSafeCharacterFinder finder;

    const auto & quoting_character = extractor_configuration.quoting_character;

    while (auto character_position_opt = finder.find_first_not(file, pos, wait_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];

        if (quoting_character == character)
        {
            return {character_position + 1u, State::READING_ENCLOSED_KEY};
        }
        else
        {
            return {character_position, State::READING_KEY};
        }
    }

    return {file.size(), State::END};
}

NextState NoEscapingKeyStateHandler::read(std::string_view file, size_t pos, ElementType & key) const
{
    BoundsSafeCharacterFinder finder;

    const auto & [key_value_delimiter, quoting_character, pair_delimiters]
        = extractor_configuration;

    key = {};

    auto start_index = pos;

    while (auto character_position_opt = finder.find_first(file, pos, read_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (character == key_value_delimiter)
        {
            key = createElement(file, start_index, character_position);

            if (key.empty())
            {
                return {next_pos, State::WAITING_KEY};
            }

            return {next_pos, State::WAITING_VALUE};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), character) != pair_delimiters.end())
        {
            return {next_pos, State::WAITING_KEY};
        }

        pos = next_pos;
    }

    return {file.size(), State::END};
}

NextState NoEscapingKeyStateHandler::readEnclosed(std::string_view file, size_t pos, ElementType & key) const
{
    BoundsSafeCharacterFinder finder;

    const auto quoting_character = extractor_configuration.quoting_character;

    key = {};

    auto start_index = pos;

    while (auto character_position_opt = finder.find_first(file, pos, read_quoted_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (quoting_character == character)
        {
            key = createElement(file, start_index, character_position);

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

NextState NoEscapingKeyStateHandler::readKeyValueDelimiter(std::string_view file, size_t pos) const
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
