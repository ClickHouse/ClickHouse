#include "StateHandler.h"
#include <string>

namespace DB
{

StateHandler::StateHandler(char escape_character_, std::optional<char> enclosing_character_)
    : escape_character(escape_character_), enclosing_character(enclosing_character_)
{
}

std::string_view StateHandler::createElement(const std::string & file, std::size_t begin, std::size_t end) const
{
    return std::string_view{file.begin() + begin, file.begin() + end};
}

}
