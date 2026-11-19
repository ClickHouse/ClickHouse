#pragma once

#include <Common/Wyrd/Namespace.h>

#include <source_location>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

void preemptionPoint(std::source_location location = std::source_location::current());

}
