#pragma once

#include <base/types.h>

namespace DB::Wyrd
{

void setRandomSeed(UInt64 seed);

UInt64 randomInt(UInt64 from, UInt64 to);
double randomDouble(double from, double to);
bool randomBool(double probability);

}
