#pragma once

#include <Common/Wyrd/Namespace.h>

#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <cstddef>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

void setRandomSeed(UInt64 seed);

UInt64 randomNumber();
UInt64 randomNumberBetween(UInt64 from, UInt64 to);
double randomReal(double from, double to);
bool randomEvent(double probability);

template <typename T>
T popRandom(VectorWithMemoryTracking<T> & items);

}
