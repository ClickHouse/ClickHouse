#pragma once

#include <string>
#include <optional>
#include "State.h"
#include "StateHandler.h"

class ValueStateHandler : StateHandler {

public:
    ValueStateHandler(char escape_character, char item_delimiter, std::optional<char> enclosing_character);

    [[nodiscard]] NextState waitValue(const std::string & file, size_t pos) const;
    [[nodiscard]] NextStateWithElement readValue(const std::string & file, size_t pos) const;
    [[nodiscard]] NextStateWithElement readEnclosedValue(const std::string & file, size_t pos) const;
    [[nodiscard]] NextStateWithElement readEmptyValue(const std::string & file, size_t pos) const;

private:
    const char item_delimiter;
};
