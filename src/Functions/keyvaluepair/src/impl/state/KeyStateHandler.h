#pragma once

#include <optional>
#include <string>

#include "State.h"
#include "StateHandler.h"

namespace DB
{

class KeyStateHandler : StateHandler
{
public:
    KeyStateHandler(char key_value_delimiter, char escape_character, std::optional<char> enclosing_character);

    [[nodiscard]] static NextState wait(std::string_view file, size_t pos) ;
    [[nodiscard]] static NextState read(std::string_view file, size_t pos, std::string_view & key);
    [[nodiscard]] static NextState readEnclosed(std::string_view file, size_t pos, std::string_view & key);
    [[nodiscard]] static NextState readKeyValueDelimiter(std::string_view file, size_t pos);

private:
    [[maybe_unused]] const char key_value_delimiter;
};

}
