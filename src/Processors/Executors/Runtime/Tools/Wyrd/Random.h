#pragma once

#include <base/types.h>

#include <cstddef>
#include <vector>

namespace DB::Runtime::Wyrd
{

UInt64 randomNumber();
bool randomEventEvery(size_t freq);
void setRandomSeed(UInt64 seed);

template <typename T>
T popRandom(std::vector<T> & items);

}
