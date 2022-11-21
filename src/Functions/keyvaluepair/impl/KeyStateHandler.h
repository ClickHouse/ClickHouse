#pragma once

#include <string>
#include <optional>

#include "State.h"
#include "StateHandler.h"

class KeyStateHandler : StateHandler {
public:
    KeyStateHandler(char key_value_delimiter, char escape_character, std::optional<char> enclosing_character);

    [[nodiscard]] NextState waitKey(const std::string & file, size_t pos) const;
    [[nodiscard]] NextStateWithElement readKey(const std::string & file, size_t pos) const;
    [[nodiscard]] NextStateWithElement readEnclosedKey(const std::string  &file, size_t pos) const;
    [[nodiscard]] NextState readKeyValueDelimiter(const std::string & file, size_t pos) const;

private:
    const char key_value_delimiter;
};
