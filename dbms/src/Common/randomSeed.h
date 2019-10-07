#pragma once

#include <cstdint>
#include <Core/Types.h>

/** Returns a number suitable as seed for PRNG. Use clock_gettime, pid and so on. */
UInt64 randomSeed();
