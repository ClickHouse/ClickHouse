#pragma once

#include <cstdint>
#include <functional>

/**
  * Sleep functions tolerant to signal interruptions (which can happen
  * when query profiler is turned on for example)
  */

void sleepForNanoseconds(uint64_t nanoseconds);

void sleepForMicroseconds(uint64_t microseconds);

void sleepForMilliseconds(uint64_t milliseconds);

void sleepForSeconds(uint64_t seconds);

/// Polls `cancellation_hook` before each sleep chunk of at most 100 ms; exceptions propagate.
/// An empty hook uses uninterrupted sleep. A zero duration does not invoke the hook.
void sleepForMilliseconds(uint64_t milliseconds, const std::function<void()> & cancellation_hook);
