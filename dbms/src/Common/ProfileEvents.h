#pragma once

#include <stddef.h>
#include <atomic>


/** Implements global counters for various events happening in the application
  *  - for high level profiling.
  * See .cpp for list of events.
  */

namespace ProfileEvents
{
    /// Event identifier (index in array).
    using Event = size_t;
    using Count = size_t;

    /// Get text description of event by identifier. Returns statically allocated string.
    const char * getDescription(Event event);

    /// Counters - how many times each event happened.
    extern std::atomic<Count> counters[];

    /// Increment a counter for event. Thread-safe.
    inline void increment(Event event, Count amount = 1)
    {
        counters[event].fetch_add(amount, std::memory_order_relaxed);
    }

    /// Get index just after last event identifier.
    Event end();
}
