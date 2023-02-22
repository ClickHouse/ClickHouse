#pragma once

#include "StateHandler.h"
#include <unordered_set>

namespace DB
{

class NoEscapingValueStateHandler : public StateHandler
{
public:
    using ElementType = std::string_view;

    NoEscapingValueStateHandler(
        char item_delimiter_,
        std::optional<char> enclosing_character_,
        std::unordered_set<char> special_character_allowlist_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & value) const;

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & value) const;

    [[nodiscard]] static NextState readEmpty(std::string_view, size_t pos, ElementType & value);

private:
    const char item_delimiter;
    std::unordered_set<char> special_character_allowlist;

    bool isValidCharacter(char character) const;
};

}
