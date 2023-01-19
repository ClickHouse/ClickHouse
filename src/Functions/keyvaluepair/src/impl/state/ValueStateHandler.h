#pragma once

#include <optional>
#include <string>
#include <unordered_set>
#include "State.h"
#include "StateHandler.h"

namespace DB
{

class ValueStateHandler : StateHandler
{
public:
    ValueStateHandler(
        char escape_character,
        char item_delimiter,
        std::optional<char> enclosing_character,
        std::unordered_set<char> special_character_allowlist_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;
    [[nodiscard]] NextState read(std::string_view file, size_t pos, std::string_view & value);
    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, std::string_view & value);
    [[nodiscard]] static NextState readEmpty(std::string_view file, size_t pos, std::string_view & value);

private:
    const char item_delimiter;
    [[maybe_unused]] std::unordered_set<char> special_character_allowlist;

    static bool isValidCharacter(char character);
};

}
