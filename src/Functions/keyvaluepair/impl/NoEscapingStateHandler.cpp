#include <Functions/keyvaluepair/impl/NoEscapingStateHandler.h>

#include <Functions/keyvaluepair/impl/NeedleFactory.h>

#include <base/find_symbols.h>
#include "Functions/keyvaluepair/impl/StateHandler.h"

namespace
{

using NextState = DB::extractKV::StateHandler::NextState;

}

namespace DB
{

namespace extractKV
{


NoEscapingStateHandler::NoEscapingStateHandler(Configuration extractor_configuration_)
: extractor_configuration(std::move(extractor_configuration_))
{
    wait_needles = NeedleFactory::getWaitNeedles(extractor_configuration);
    read_needles = NeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = NeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState NoEscapingStateHandler::waitKey(std::string_view file) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    if (const auto * p = find_first_not_symbols_or_null(file, wait_needles))
    {
        const size_t character_position = p - file.begin();
        if (*p == quoting_character)
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

NextState NoEscapingStateHandler::readKey(std::string_view file, StringWriter & key) const
{
    const auto & [key_value_delimiter, _, pair_delimiters] = extractor_configuration;

    key.reset();

    size_t pos = 0;
    auto start_index = pos;

    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
    {
        const size_t character_position = p - file.begin();
        auto next_pos = character_position + 1u;

        if (*p == key_value_delimiter)
        {
            key.append(file.begin() + start_index, file.begin() + character_position);

            if (key.isEmpty())
            {
                return {next_pos, State::WAITING_KEY};
            }

            return {next_pos, State::WAITING_VALUE};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), *p) != pair_delimiters.end())
        {
            return {next_pos, State::WAITING_KEY};
        }

        pos = next_pos;
        // TODO: add check to not read past end of `file`'s data?
    }

    return {file.size(), State::END};
}

NextState NoEscapingStateHandler::readQuotedKey(std::string_view file, StringWriter & key) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    key.reset();

    size_t pos = 0;
    auto start_index = pos;

    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
    {
        size_t character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == quoting_character)
        {
            key.append(file.begin() + start_index, file.begin() + character_position);

            if (key.isEmpty())
            {
                return {next_pos, State::WAITING_KEY};
            }

            return {next_pos, State::READING_KV_DELIMITER};
        }

        pos = next_pos;
    }

    return {file.size(), State::END};
}

NextState NoEscapingStateHandler::readKeyValueDelimiter(std::string_view file) const
{
    if (!file.empty())
    {
        const auto current_character = file[0];

        if (current_character == extractor_configuration.key_value_delimiter)
        {
            return {1, WAITING_VALUE};
        }
    }

    return {0, State::WAITING_KEY};
}

NextState NoEscapingStateHandler::waitValue(std::string_view file) const
{
    const auto & [key_value_delimiter, quoting_character, _] = extractor_configuration;

    size_t pos = 0;

    if (!file.empty())
    {
        const auto current_character = file[pos];

        if (current_character == quoting_character)
        {
            return {pos + 1u, State::READING_QUOTED_VALUE};
        }
        else if (current_character == key_value_delimiter)
        {
            return {pos, State::WAITING_KEY};
        }
    }

    return {pos, State::READING_VALUE};
}

NextState NoEscapingStateHandler::readValue(std::string_view file, StringWriter & value) const
{
    const auto & [key_value_delimiter, _, pair_delimiters] = extractor_configuration;

    value.reset();

    size_t pos = 0;

    auto start_index = pos;

    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
    {
        const size_t character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == key_value_delimiter)
        {
            return {next_pos, State::WAITING_KEY};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), *p) != pair_delimiters.end())
        {
            // reached next pair
            value.append(file.begin() + start_index, file.begin() + character_position);

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    // TODO: do I really need the below logic?
    // this allows empty values at the end
    value.append(file.begin() + start_index, file.end());
    return {file.size(), State::FLUSH_PAIR};
}

NextState NoEscapingStateHandler::readQuotedValue(std::string_view file, StringWriter & value) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    size_t pos = 0;
    auto start_index = pos;

    value.reset();

    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
    {
        const size_t character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == quoting_character)
        {
            value.append(file.begin() + start_index, file.begin() + character_position);

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    return {file.size(), State::END};
}

}

}
