#pragma once

#include <cstdint>

/** Returns a number suitable as seed for PRNG. Use clock_gettime, pid and so on. */
uint64_t randomSeed();
