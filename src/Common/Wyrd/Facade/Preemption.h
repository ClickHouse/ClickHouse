#pragma once

#include <source_location>

namespace DB::Wyrd
{

void preemptionPoint(std::source_location location = std::source_location::current());

}
