#include "NoEscapingValueStateHandler.h"
#include <Functions/keyvaluepair/src/impl/state/strategies/util/CharacterFinder.h>
#include <Functions/keyvaluepair/src/impl/state/strategies/util/NeedleFactory.h>

namespace DB
{

NoEscapingValueStateHandler::NoEscapingValueStateHandler(ExtractorConfiguration extractor_configuration_)
    : extractor_configuration(std::move(extractor_configuration_))
{
    read_needles = NeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = NeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState NoEscapingValueStateHandler::wait(std::string_view file, size_t pos) const
{
    const auto & [key_value_delimiter, pair_delimiters, quoting_characters]
        = extractor_configuration;

    if (pos < file.size())
    {
        const auto current_character = file[pos];

        if (std::find(quoting_characters.begin(), quoting_characters.end(), current_character) != quoting_characters.end())
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

    return {file.size(), State::READING_EMPTY_VALUE};
}

NextState NoEscapingValueStateHandler::read(std::string_view file, size_t pos, ElementType & value) const
{
    auto start_index = pos;

    value = {};

    BoundsSafeCharacterFinder finder;

    const auto & [key_value_delimiter, pair_delimiters, quoting_characters]
        = extractor_configuration;

    while (auto character_position_opt = finder.find_first(file, pos, read_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (key_value_delimiter == character)
        {
            return {next_pos, State::WAITING_KEY};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), character) != pair_delimiters.end())
        {
            value = createElement(file, start_index, character_position);
            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    // TODO: do I really need the below logic?
    // this allows empty values at the end
    value = createElement(file, start_index, file.size());
    return {file.size(), State::FLUSH_PAIR};
}

NextState NoEscapingValueStateHandler::readEnclosed(std::string_view file, size_t pos, ElementType & value) const
{
    auto start_index = pos;

    value = {};
    BoundsSafeCharacterFinder finder;

    const auto & quoting_characters = extractor_configuration.quoting_characters;

    while (auto character_position_opt = finder.find_first(file, pos, read_quoted_needles))
    {
        auto character_position = *character_position_opt;
        auto character = file[character_position];
        auto next_pos = character_position + 1u;

        if (std::find(quoting_characters.begin(), quoting_characters.end(), character) != quoting_characters.end())
        {
            value = createElement(file, start_index, character_position);

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    return {file.size(), State::END};
}

NextState NoEscapingValueStateHandler::readEmpty(std::string_view, size_t pos, ElementType & value)
{
    value = {};
    return {pos + 1, State::FLUSH_PAIR};
}

}
