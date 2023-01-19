#pragma once

#include <optional>
#include <string>
#include "State.h"
#include "StateHandler.h"

namespace DB
{

class InlineEscapingKeyStateHandler : public StateHandler
{
public:
    using Key = std::string;
    InlineEscapingKeyStateHandler(char key_value_delimiter, char escape_character, std::optional<char> enclosing_character);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) ;
    [[nodiscard]] NextState read(std::string_view file, size_t pos, Key & key) const;
    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, Key & key);
    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const;

private:
    [[maybe_unused]] const char key_value_delimiter;
};

}
