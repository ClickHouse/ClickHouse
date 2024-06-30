#pragma once

#include <cstdint>
#include <base/types.h>

/** Returns a number suitable as seed for PRNG. Use clock_gettime, pid and so on. */
UInt64 randomSeed();
