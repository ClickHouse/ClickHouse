#pragma once

#include <string>
#include "State.h"
#include "StateHandler.h"

namespace DB
{

template <typename ValueStateHandler>
concept CInlineEscapingValueStateHandler = requires(ValueStateHandler handler)
{
    { handler.wait(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readEnclosed(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readEmpty(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
};

template <QuotingStrategy QUOTING_STRATEGY, EscapingStrategy ESCAPING_STRATEGY>
class InlineEscapingValueStateHandler : public StateHandler
{
    using Value = std::string;

public:
    InlineEscapingValueStateHandler(
        char escape_character_,
        char item_delimiter_,
        std::optional<char> enclosing_character_)
        : StateHandler(escape_character_, enclosing_character_)
        , item_delimiter(item_delimiter_)
    {
    }

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const
    {
        while (pos < file.size())
        {
            const auto current_character = file[pos];

            if constexpr (QuotingStrategy::WithQuoting == QUOTING_STRATEGY)
            {
                if (current_character == enclosing_character)
                {
                    return {pos + 1u, State::READING_ENCLOSED_VALUE};
                }
            }
            if (current_character == item_delimiter)
            {
                return {pos, State::READING_EMPTY_VALUE};
            }
            else if (isValidCharacter(current_character))
            {
                return {pos, State::READING_VALUE};
            }
            else
            {
                pos++;
            }
        }

        return {pos, State::READING_EMPTY_VALUE};
    }

    [[nodiscard]] NextState read(std::string_view file, size_t pos, Value & value)
    {
        bool escape = false;

        value.clear();

        while (pos < file.size())
        {
            const auto current_character = file[pos++];

            if constexpr (EscapingStrategy::WithEscaping == ESCAPING_STRATEGY)
            {
                if (escape)
                {
                    escape = false;
                    value.push_back(current_character);
                    continue;
                }
                else if (escape_character == current_character)
                {
                    escape = true;
                    continue;
                }
            }
            if (current_character == item_delimiter || !isValidCharacter(current_character))
            {
                return {pos, State::FLUSH_PAIR};
            }
            else
            {
                value.push_back(current_character);
            }
        }

        return {pos, State::FLUSH_PAIR};
    }

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, Value & value)
    {
        value.clear();

        while (pos < file.size())
        {
            const auto current_character = file[pos++];
            if (enclosing_character == current_character)
            {
                return {pos, State::FLUSH_PAIR};
            }
            else
            {
                value.push_back(current_character);
            }
        }

        return {pos, State::END};
    }

    [[nodiscard]] static NextState readEmpty(std::string_view, size_t pos, Value & value)
    {
        value.clear();
        return {pos + 1, State::FLUSH_PAIR};
    }

private:
    const char item_delimiter;

    bool isValidCharacter(char character) const
    {
        if constexpr (ESCAPING_STRATEGY == EscapingStrategy::WithEscaping)
        {
            if (character == escape_character)
            {
                return true;
            }
        }
        return /*special_character_allowlist.contains(character) ||*/ std::isalnum(character) || character == '_' || character == '.';
    }
};

}
