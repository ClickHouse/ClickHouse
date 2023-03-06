#pragma once

#include <optional>
#include <string>
#include "StateHandler.h"

namespace DB
{

class InlineEscapingKeyStateHandler : public StateHandler
{
public:
    using ElementType = std::string;

    InlineEscapingKeyStateHandler(char key_value_delimiter_, std::optional<char> enclosing_character_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const;

private:
    const char key_value_delimiter;

    static bool isValidCharacter(char character);
};

}
