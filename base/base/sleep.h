#pragma once

#include <cstdint>
#include <functional>

/**
  * Sleep functions tolerant to signal interruptions (which can happen
  * when query profiler is turned on for example)
  */

/// Blocks the calling thread for the given duration in nanoseconds, conversion to nanoseconds must fit in `uint64_t`.
void sleepForSeconds(uint64_t seconds);
void sleepForMilliseconds(uint64_t milliseconds);
void sleepForMicroseconds(uint64_t microseconds);
void sleepForNanoseconds(uint64_t nanoseconds);

/// Polls `cancellation_hook` before each sleep chunk of at most 100 ms; exceptions propagate.
/// An empty hook uses uninterrupted sleep. A zero duration does not invoke the hook.
void sleepForMilliseconds(uint64_t milliseconds, const std::function<void()> & cancellation_hook);
