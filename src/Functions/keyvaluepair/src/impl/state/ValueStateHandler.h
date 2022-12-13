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

    [[nodiscard]] NextState wait(const std::string & file, size_t pos) const;
    [[nodiscard]] NextState read(const std::string & file, size_t pos);
    [[nodiscard]] NextState readEnclosed(const std::string & file, size_t pos);
    [[nodiscard]] NextState readEmpty(const std::string & file, size_t pos);

    [[nodiscard]] std::string_view get() const override;

private:
    const char item_delimiter;
    std::unordered_set<char> special_character_allowlist;
    std::string_view value;

    bool isValidCharacter(char character) const;
};

}
