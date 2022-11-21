#pragma once

#include <optional>
#include <string_view>

struct StateHandler {
    StateHandler(char escape_character, std::optional<char> enclosing_character);

    const char escape_character;
    const std::optional<char> enclosing_character;

    [[nodiscard]] std::string_view createElement(const std::string & file, std::size_t begin, std::size_t end) const;
};
