#pragma once

#include <Common/Wyrd/Namespace.h>

#include <source_location>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

void injectFault(const char * operation, std::source_location location = std::source_location::current());
bool spuriousFailure(std::source_location location = std::source_location::current());

}
