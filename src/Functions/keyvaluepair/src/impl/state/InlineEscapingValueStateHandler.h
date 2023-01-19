#pragma once

#include <string>
#include "State.h"
#include "StateHandler.h"

namespace DB
{

class InlineEscapingValueStateHandler : public StateHandler
{
    using Value = std::string;

public:
    InlineEscapingValueStateHandler(
        char escape_character,
        char item_delimiter,
        std::optional<char> enclosing_character);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;
    [[nodiscard]] NextState read(std::string_view file, size_t pos, Value & value);
    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, Value & value);
    [[nodiscard]] static NextState readEmpty(std::string_view file, size_t pos, Value & value);

private:
    const char item_delimiter;

    bool isValidCharacter(char character) const;
};

}
