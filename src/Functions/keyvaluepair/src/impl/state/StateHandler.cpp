#include "StateHandler.h"

namespace DB
{

StateHandler::StateHandler(std::optional<char> enclosing_character_)
    : enclosing_character(enclosing_character_)
{
}

std::string_view StateHandler::createElement(std::string_view file, std::size_t begin, std::size_t end)
{
    return std::string_view{file.begin() + begin, file.begin() + end};
}

}
