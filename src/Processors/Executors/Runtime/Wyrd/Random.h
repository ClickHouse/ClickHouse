#pragma once

#include <base/types.h>

#include <cstddef>
#include <vector>

namespace DB::Runtime::Wyrd
{

void setRandomSeed(UInt64 seed);

UInt64 randomNumber();
bool randomEventEvery(size_t freq);

template <typename T>
T popRandom(std::vector<T> & items);

}
