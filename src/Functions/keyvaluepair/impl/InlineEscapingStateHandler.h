#pragma once

#include <Functions/keyvaluepair/impl/Configuration.h>
#include <Functions/keyvaluepair/impl/StateHandler.h>
#include <Functions/keyvaluepair/impl/StringWriter.h>
#include <Functions/keyvaluepair/impl/NeedleFactory.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <base/find_symbols.h>

#include <string_view>
#include <string>
#include <vector>

namespace DB
{

namespace extractKV
{

namespace
{
    std::pair<bool, std::size_t> consumeWithEscapeSequence(std::string_view file, size_t start_pos, size_t character_pos, DB::extractKV::StringWriter & output)
    {
        std::string escaped_sequence;
        DB::ReadBufferFromMemory buf(file.begin() + character_pos, file.size() - character_pos);

        if (DB::parseComplexEscapeSequence(escaped_sequence, buf))
        {
            output.append(file.begin() + start_pos, file.begin() + character_pos);
            output.append(escaped_sequence);

            return {true, buf.getPosition()};
        }


        return {false, buf.getPosition()};
    }

    using NextState = DB::extractKV::StateHandler::NextState;

}

template <bool WITH_ESCAPING>
class InlineEscapingStateHandler : public StateHandler
{
public:
    explicit InlineEscapingStateHandler(Configuration configuration_)
        : configuration(std::move(configuration_))
    {
        // improve below, possibly propagating with_Escaping to needle factory as well
        if constexpr (WITH_ESCAPING)
        {
            wait_needles = EscapingNeedleFactory::getWaitNeedles(configuration);
            read_needles = EscapingNeedleFactory::getReadNeedles(configuration);
            read_quoted_needles = EscapingNeedleFactory::getReadQuotedNeedles(configuration);
        }
        else
        {
            wait_needles = NeedleFactory::getWaitNeedles(configuration);
            read_needles = NeedleFactory::getReadNeedles(configuration);
            read_quoted_needles = NeedleFactory::getReadQuotedNeedles(configuration);
        }
    }

    [[nodiscard]] NextState waitKey(std::string_view file) const
    {
        const auto quoting_character = configuration.quoting_character;

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

    [[nodiscard]] NextState readKey(std::string_view file, StringWriter & key) const
    {
        const auto & [key_value_delimiter, _, pair_delimiters] = configuration;

        key.reset();

        size_t pos = 0;

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
        {
            auto character_position = p - file.begin();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && *p == '\\')
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence(file, pos, character_position, key);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        return {next_pos, State::WAITING_KEY};
                    }
                }
            }
            else if (*p == key_value_delimiter)
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

    [[nodiscard]] NextState readQuotedKey(std::string_view file, StringWriter & key) const
    {
        const auto quoting_character = configuration.quoting_character;

        key.reset();

        size_t pos = 0;

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
        {
            size_t character_position = p - file.begin();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && *p == '\\')
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence(file, pos, character_position, key);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        return {next_pos, State::WAITING_KEY};
                    }
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

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file) const
    {
        if (!file.empty())
        {
            const auto current_character = file[0];

            if (current_character == configuration.key_value_delimiter)
            {
                return {1, WAITING_VALUE};
            }
        }

        return {0, State::WAITING_KEY};
    }

    [[nodiscard]] NextState waitValue(std::string_view file) const
    {
        const auto & [key_value_delimiter, quoting_character, _] = configuration;

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

    [[nodiscard]] NextState readValue(std::string_view file, StringWriter & value) const
    {
        const auto & [key_value_delimiter, _, pair_delimiters] = configuration;

        value.reset();

        size_t pos = 0;

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_needles))
        {
            const size_t character_position = p - file.begin();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && *p == '\\')
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence(file, pos, character_position, value);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        return {next_pos, State::WAITING_KEY};
                    }
                }
            }
            else if (*p == key_value_delimiter)
            {
                return {next_pos, State::WAITING_KEY};
            }
            else if (std::find(pair_delimiters.begin(), pair_delimiters.end(), *p) != pair_delimiters.end())
            {
                value.append(file.begin() + pos, file.begin() + character_position);

                return {next_pos, State::FLUSH_PAIR};
            }

            pos = next_pos;
        }

        // Reached end of input, consume rest of the file as value and make sure KV pair is produced.
        value.append(file.begin() + pos, file.end());
        return {file.size(), State::FLUSH_PAIR};
    }

    [[nodiscard]] NextState readQuotedValue(std::string_view file, StringWriter & value) const
    {
        const auto quoting_character = configuration.quoting_character;

        size_t pos = 0;

        value.reset();

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
        {
            const size_t character_position = p - file.begin();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && *p == '\\')
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence(file, pos, character_position, value);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        return {next_pos, State::WAITING_KEY};
                    }
                }
            }
            else if (*p == quoting_character)
            {
                value.append(file.begin() + pos, file.begin() + character_position);

                return {next_pos, State::FLUSH_PAIR};
            }

            pos = next_pos;
        }

        return {file.size(), State::END};
    }

    const Configuration configuration;

private:
    std::vector<char> wait_needles;
    std::vector<char> read_needles;
    std::vector<char> read_quoted_needles;
};

}

}
