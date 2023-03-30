#include <Functions/keyvaluepair/impl/NoEscapingStateHandler.h>

#include <Functions/keyvaluepair/impl/NeedleFactory.h>

#include <base/find_symbols.h>

namespace
{

std::string_view createElement(std::string_view file, std::size_t begin, std::size_t end)
{
    return std::string_view{file.begin() + begin, file.begin() + end};
}

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

NextState NoEscapingStateHandler::readKey(std::string_view file, KeyType & key) const
{
    const auto & [key_value_delimiter, _, pair_delimiters] = extractor_configuration;

    key = {};

    size_t pos = 0;
    auto start_index = pos;

    while (auto p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
    {
        const size_t character_position = p - file.begin();
        auto next_pos = character_position + 1u;

        if (*p == key_value_delimiter)
        {
            key = createElement(file, start_index, character_position);

            if (key.empty())
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

NextState NoEscapingStateHandler::readQuotedKey(std::string_view file, KeyType & key) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    key = {};

    size_t pos = 0;
    auto start_index = pos;

    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
    {
        const size_t character_position = p - file.begin();
        auto next_pos = character_position + 1u;

        if (*p == quoting_character)
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

NextState NoEscapingStateHandler::readKeyValueDelimiter(std::string_view file) const
{
    const auto current_character = file[0];
    if (current_character == extractor_configuration.key_value_delimiter)
    {
        return {1, WAITING_VALUE};
    }

    return {0, State::WAITING_KEY};
}

NextState NoEscapingStateHandler::waitValue(std::string_view file) const
{
    const auto & [key_value_delimiter, quoting_character, _] = extractor_configuration;

    size_t pos = 0;
    const auto current_character = file[pos];

    if (current_character == quoting_character)
    {
        return {pos + 1u, State::READING_QUOTED_VALUE};
    }
    else if (current_character == key_value_delimiter)
    {
        return {pos, State::WAITING_KEY};
    }

    return {pos, State::READING_VALUE};
}

NextState NoEscapingStateHandler::readValue(std::string_view file, ValueType & value) const
{
    const auto & [key_value_delimiter, _, pair_delimiters] = extractor_configuration;

    value = {};

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

NextState NoEscapingStateHandler::readQuotedValue(std::string_view file, ValueType & value) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    size_t pos = 0;
    auto start_index = pos;

    value = {};

    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
    {
        const size_t character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == quoting_character)
        {
            value = createElement(file, start_index, character_position);

            std::cerr << "NoEscapingStateHandler::readQuoted Going to consume up to: «" << fancyQuote(file.substr(0, next_pos)) << " to " << fancyQuote(file.substr(next_pos)) << std::endl;
            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    return {file.size(), State::END};
}

}

}
