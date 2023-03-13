#include "StateHandler.h"

namespace DB
{

std::string_view StateHandler::createElement(std::string_view file, std::size_t begin, std::size_t end)
{
    return std::string_view{file.begin() + begin, file.begin() + end};
}

}
