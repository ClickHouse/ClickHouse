#pragma once

#include <cstdint>

/**
  * Sleep functions tolerant to signal interruptions (which can happen
  * when query profiler is turned on for example)
  */

void sleepForNanoseconds(uint64_t nanoseconds);

void sleepForMicroseconds(uint64_t microseconds);

void sleepForMilliseconds(uint64_t milliseconds);

void sleepForSeconds(uint64_t seconds);
