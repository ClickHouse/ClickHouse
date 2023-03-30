#include <Functions/keyvaluepair/impl/InlineEscapingStateHandler.h>
#include <Functions/keyvaluepair/impl/NeedleFactory.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <base/find_symbols.h>

namespace
{
size_t consumeWithEscapeSequence(std::string_view file, size_t start_pos, size_t character_pos, DB::extractKV::StringWriter & output)
{
    output.append(file.begin() + start_pos, file.begin() + character_pos);

    std::string tmp_out;
    DB::ReadBufferFromMemory buf(file.begin() + character_pos, file.size() - character_pos);

    DB::parseComplexEscapeSequence(tmp_out, buf);
    output.append(tmp_out);

    return buf.getPosition();
}

using NextState = DB::extractKV::StateHandler::NextState;

}

namespace DB
{

namespace extractKV
{

InlineEscapingStateHandler::InlineEscapingStateHandler(Configuration extractor_configuration_)
    : extractor_configuration(std::move(extractor_configuration_))
{
    wait_needles = EscapingNeedleFactory::getWaitNeedles(extractor_configuration);
    read_needles = EscapingNeedleFactory::getReadNeedles(extractor_configuration);
    read_quoted_needles = EscapingNeedleFactory::getReadQuotedNeedles(extractor_configuration);
}

NextState InlineEscapingStateHandler::waitKey(std::string_view file) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    if (const auto * p = find_first_not_symbols_or_null(file, wait_needles))
    {
        const size_t character_position = p - file.begin();
        if (*p == quoting_character)
        {
            // +1 to skip quoting character
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

NextState InlineEscapingStateHandler::readKey(std::string_view file, StringWriter & key) const
{
    const auto & [key_value_delimiter, _, pair_delimiters] = extractor_configuration;

    key.reset();

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
        else
        if (*p == key_value_delimiter)
        {
            key.append(file.begin() + pos, file.begin() + character_position);

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

NextState InlineEscapingStateHandler::readQuotedKey(std::string_view file, StringWriter & key) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    key.reset();

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
            key.append(file.begin() + pos, file.begin() + character_position);

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

NextState InlineEscapingStateHandler::readKeyValueDelimiter(std::string_view file) const
{
    const auto current_character = file[0];
    if (current_character == extractor_configuration.key_value_delimiter)
    {
        return {1, WAITING_VALUE};
    }

    return {0, State::WAITING_KEY};
}

NextState InlineEscapingStateHandler::waitValue(std::string_view file) const
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

NextState InlineEscapingStateHandler::readValue(std::string_view file, StringWriter & value) const
{
    const auto & [key_value_delimiter, _, pair_delimiters] = extractor_configuration;

    value.reset();

    size_t pos = 0;
    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
    {
        const size_t character_position = p - file.begin();
        size_t next_pos = character_position + 1u;

        if (*p == '\\')
        {
            const size_t escape_seq_len = consumeWithEscapeSequence(file, pos, character_position, value);
            next_pos = character_position + escape_seq_len;
            if (escape_seq_len == 0)
            {
                // It is agreed that value with an invalid escape seqence in it
                // is considered malformed and shoudn't be included in result.
                value.reset();
                return {next_pos, State::WAITING_KEY};
            }
        }
        else
        if (*p == key_value_delimiter)
        {
            // reached new key
            return {next_pos, State::WAITING_KEY};
        }
        else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), *p) != pair_delimiters.end())
        {
            // reached next pair
            value.append(file.begin() + pos, file.begin() + character_position);

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    // Reached end of input, consume rest of the file as value and make sure KV pair is produced.
    value.append(file.begin() + pos, file.end());
    return {file.size(), State::FLUSH_PAIR};
}

NextState InlineEscapingStateHandler::readQuotedValue(std::string_view file, StringWriter & value) const
{
    const auto quoting_character = extractor_configuration.quoting_character;

    size_t pos = 0;

    value.reset();

    while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
    {
        const size_t character_position = p - file.begin();
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
        else
        if (*p == quoting_character)
        {
            value.append(file.begin() + pos, file.begin() + character_position);

            return {next_pos, State::FLUSH_PAIR};
        }

        pos = next_pos;
    }

    return {file.size(), State::END};
}

}

}
