#include "InlineEscapingStateHandler.h"
#include <Functions/keyvaluepair/src/impl/state/strategies/util/CharacterFinder.h>
#include <Functions/keyvaluepair/src/impl/state/strategies/util/NeedleFactory.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <base/find_symbols.h>

namespace
{
size_t consumeWithEscapeSequence(std::string_view file, size_t start_pos, size_t character_pos, std::string & output)
{
    output.insert(output.end(), file.begin() + start_pos, file.begin() + character_pos);

    DB::ReadBufferFromMemory buf(file.begin() + character_pos, file.size() - character_pos);
    DB::parseComplexEscapeSequence(output, buf);

    return buf.getPosition();
}
}

namespace DB
{

InlineEscapingKeyStateHandler::InlineEscapingKeyStateHandler(Configuration configuration_)
    : extractor_configuration(std::move(configuration_))
{
    wait_needles = EscapingNeedleFactory::getWaitNeedles(extractor_configuration);
    read_needles = EscapingNeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = EscapingNeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState InlineEscapingKeyStateHandler::wait(std::string_view file) const
{
    const auto quoting_character = extractor_configuration.quoting_character;
    size_t pos = 0;
    while (const auto * p = find_first_not_symbols_or_null({file.begin() + pos, file.end()}, wait_needles))
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

/*
 * I only need to iteratively copy stuff if there are escape sequences. If not, views are sufficient.
 * TSKV has a nice catch for that, implementers kept an auxiliary string to hold copied characters.
 * If I find a key value delimiter and that is empty, I do not need to copy? hm,m hm hm
 * */

NextState InlineEscapingKeyStateHandler::read(std::string_view file, ElementType & key) const
{
    const auto & [key_value_delimiter, quoting_character, pair_delimiters] = extractor_configuration;

    key.clear();

    size_t pos = 0;
    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
    {
        auto character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == '\\')
        {
            const size_t escape_seq_len = consumeWithEscapeSequence(file, pos, character_position, key);
            next_pos = character_position + escape_seq_len;
            if (escape_seq_len == 0)
            {
                return {next_pos, State::WAITING_KEY};
            }
        }
        else if (*p == key_value_delimiter)
        {
            key.insert(key.end(), file.begin() + pos, file.begin() + character_position);

            return {next_pos, State::WAITING_VALUE};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), *p) != pair_delimiters.end())
        {
            return {next_pos, State::WAITING_KEY};
        }

        pos = next_pos;
    }

    // might be problematic in case string reaches the end and I haven't copied anything over to key

    return {file.size(), State::END};
}

NextState InlineEscapingKeyStateHandler::readQuoted(std::string_view file, ElementType & key) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    key.clear();

    size_t pos = 0;
    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
    {
        size_t character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == '\\')
        {
            const size_t escape_seq_len = consumeWithEscapeSequence(file, pos, character_position, key);
            next_pos = character_position + escape_seq_len;

            if (escape_seq_len == 0)
            {
                return {next_pos, State::WAITING_KEY};
            }
        }
        else if (*p == quoting_character)
        {
            key.insert(key.end(), file.begin() + pos, file.begin() + character_position);

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

NextState InlineEscapingKeyStateHandler::readKeyValueDelimiter(std::string_view file) const
{
    const auto current_character = file[0];
    if (current_character == extractor_configuration.key_value_delimiter)
    {
        return {1, WAITING_VALUE};
    }

    return {0, State::WAITING_KEY};
}


InlineEscapingValueStateHandler::InlineEscapingValueStateHandler(Configuration extractor_configuration_)
    : extractor_configuration(std::move(extractor_configuration_))
{
    read_needles = EscapingNeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = EscapingNeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState InlineEscapingValueStateHandler::wait(std::string_view file) const
{
    const auto & [key_value_delimiter, quoting_character, pair_delimiters] = extractor_configuration;

    size_t pos = 0;
    if (pos < file.size())
    {
        const auto current_character = file[pos];

        if (quoting_character == current_character)
        {
            return {pos + 1u, State::READING_QUOTED_VALUE};
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

    return {pos, State::READING_VALUE};
}

NextState InlineEscapingValueStateHandler::read(std::string_view file, ElementType & value) const
{
    const auto & [key_value_delimiter, quoting_character, pair_delimiters] = extractor_configuration;

    value.clear();

    size_t pos = 0;
    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
    {
        auto character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == '\\')
        {
            const size_t escape_seq_len = consumeWithEscapeSequence(file, pos, character_position, value);
            next_pos = character_position + escape_seq_len;
            if (escape_seq_len == 0)
            {
                return {next_pos, State::WAITING_KEY};
            }
        }
        else if (*p == key_value_delimiter)
        {
            // reached new key
            return {next_pos, State::WAITING_KEY};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), *p) != pair_delimiters.end())
        {
            // reached next pair
            value.insert(value.end(), file.begin() + pos, file.begin() + character_position);

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    // Reached end of input, consume rest of the file as value and make sure KV pair is produced.
    value.insert(value.end(), file.begin() + pos, file.end());

    return {pos, State::FLUSH_PAIR};
}

NextState InlineEscapingValueStateHandler::readQuoted(std::string_view file, ElementType & value) const
{
    const auto quoting_character = extractor_configuration.quoting_character;
    const std::string_view needles{read_quoted_needles.begin(), read_quoted_needles.end()};

    value.clear();

    size_t pos = 0;
    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, needles))
    {
        auto character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == '\\')
        {
            const size_t escape_seq_len = consumeWithEscapeSequence(file, pos, character_position, value);
            next_pos = character_position + escape_seq_len;
            if (escape_seq_len == 0)
            {
                return {next_pos, State::WAITING_KEY};
            }
        }
        else if (*p == quoting_character)
        {
            value.insert(value.end(), file.begin() + pos, file.begin() + character_position);

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    return {pos, State::END};
}

}
