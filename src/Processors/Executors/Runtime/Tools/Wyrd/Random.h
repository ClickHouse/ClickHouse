#pragma once

#include <base/types.h>

#include <cstddef>

namespace DB::Runtime::Wyrd
{

UInt64 randomNumber();
bool randomEventEvery(size_t freq);
void setRandomSeed(UInt64 seed);

}
