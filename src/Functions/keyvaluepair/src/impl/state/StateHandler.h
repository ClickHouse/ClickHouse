#pragma once

#include <optional>
#include <string_view>

namespace DB
{

enum QuotingStrategy
{
    WithQuoting,
    WithoutQuoting
};

enum EscapingStrategy
{
    WithEscaping,
    WithoutEscaping
};

struct StateHandler
{
    StateHandler(char escape_character, std::optional<char> enclosing_character);
    StateHandler(const StateHandler &) = default;

    virtual ~StateHandler() = default;

    const char escape_character = '\\';
    const std::optional<char> enclosing_character;

protected:
    [[nodiscard]] static std::string_view createElement(std::string_view file, std::size_t begin, std::size_t end);
};

}
