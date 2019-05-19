#pragma once

#include <cstdint>
#include <time.h>
#include <errno.h>

void SleepForNanoseconds(uint64_t nanoseconds);

void SleepForMicroseconds(uint64_t microseconds);

void SleepForMilliseconds(uint64_t milliseconds);

void SleepForSeconds(uint64_t seconds);
