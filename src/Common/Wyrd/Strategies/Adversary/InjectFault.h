#pragma once

#include <source_location>

namespace DB::Wyrd::AdversaryMode
{

void injectFault(const char * operation, std::source_location location = std::source_location::current());
bool spuriousFailure(std::source_location location = std::source_location::current());

}
