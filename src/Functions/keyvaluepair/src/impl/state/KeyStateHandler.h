#pragma once

#include <string>
#include <optional>

#include "State.h"
#include "StateHandler.h"

namespace DB
{

class KeyStateHandler : StateHandler {
public:
    KeyStateHandler(char key_value_delimiter, char escape_character, std::optional<char> enclosing_character);

    [[nodiscard]] NextState wait(const std::string & file, size_t pos) const;
    [[nodiscard]] NextStateWithElement read(const std::string & file, size_t pos) const;
    [[nodiscard]] NextStateWithElement readEnclosed(const std::string  &file, size_t pos) const;
    [[nodiscard]] NextState readKeyValueDelimiter(const std::string & file, size_t pos) const;

private:
    const char key_value_delimiter;
};

}
