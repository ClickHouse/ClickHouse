#pragma once

#include <source_location>
#include <string>

namespace DB::Runtime::Wyrd
{

void log(std::string message, std::source_location location = std::source_location::current());

}
