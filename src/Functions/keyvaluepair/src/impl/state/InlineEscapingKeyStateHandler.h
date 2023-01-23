#pragma once

#include <optional>
#include <string>
#include "State.h"
#include "StateHandler.h"

namespace DB
{

template <typename KeyStateHandler>
concept CInlineEscapingKeyStateHandler = requires(KeyStateHandler handler)
{
    { handler.wait(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readEnclosed(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readKeyValueDelimiter(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
};

template <QuotingStrategy QUOTING_STRATEGY, EscapingStrategy ESCAPING_STRATEGY>
class InlineEscapingKeyStateHandler : public StateHandler
{
public:
    using Key = std::string;
    InlineEscapingKeyStateHandler(char key_value_delimiter_, char escape_character_, std::optional<char> enclosing_character_)
        : StateHandler(escape_character_, enclosing_character_), key_value_delimiter(key_value_delimiter_)
    {
    }

    [[nodiscard]] NextState wait(std::string_view file, size_t pos)
    {
        while (pos < file.size())
        {
            const auto current_character = file[pos];

            if constexpr (ESCAPING_STRATEGY == EscapingStrategy::WithEscaping)
            {
                if (current_character == escape_character)
                {
                    return {pos, State::READING_KEY};
                }
            }

            if (isalnum(current_character))
            {
                return {pos, State::READING_KEY};
            }

            if constexpr (QUOTING_STRATEGY == QuotingStrategy::WithQuoting)
            {
                if (current_character == enclosing_character)
                {
                    return {pos + 1u, State::READING_ENCLOSED_KEY};
                }
            }

            pos++;
        }

        return {pos, State::END};
    }

    [[nodiscard]] NextState read(std::string_view file, size_t pos, Key & key) const
    {
        bool escape = false;

        key.clear();

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if constexpr (ESCAPING_STRATEGY == EscapingStrategy::WithEscaping)
            {
                if (escape)
                {
                    key.push_back(current_character);
                    escape = false;
                    continue;
                }
                else if (escape_character == current_character)
                {
                    escape = true;
                    continue;
                }
            }

            if (current_character == key_value_delimiter)
            {
                return {pos, State::WAITING_VALUE};
            }
            else if (!std::isalnum(current_character) && current_character != '_')
            {
                return {pos, State::WAITING_KEY};
            }
            else
            {
                key.push_back(current_character);
            }
        }

        return {pos, State::END};
    }

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, Key & key)
    {
        key.clear();

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if (*enclosing_character == current_character)
            {
                if (key.empty())
                {
                    return {pos, State::WAITING_KEY};
                }

                return {pos, State::READING_KV_DELIMITER};
            }
            else
            {
                key.push_back(current_character);
            }
        }

        return {pos, State::END};
    }

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const
    {
        if (pos == file.size())
        {
            return {pos, State::END};
        }
        else
        {
            const auto current_character = file[pos++];
            return {pos, current_character == key_value_delimiter ? State::WAITING_VALUE : State::WAITING_KEY};
        }
    }

private:
    [[maybe_unused]] const char key_value_delimiter;
};

}
