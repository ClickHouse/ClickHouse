#pragma once

#include <base/types.h>

namespace DB::Wyrd
{

UInt64 randomNumber();
UInt64 randomNumberBetween(UInt64 from, UInt64 to);
double randomReal(double from, double to);
bool randomEvent(double probability);

}
