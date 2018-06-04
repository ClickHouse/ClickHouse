#pragma once

#include <stddef.h>
#include <atomic>
#include <memory>

namespace DB
{

class IColumn;

}

/** Implements global counters for various events happening in the application
  *  - for high level profiling.
  * See .cpp for list of events.
  */

namespace ProfileEvents
{
    /// Event identifier (index in array).
    using Event = size_t;
    using Count = size_t;
    using Counter = std::atomic<Count>;
    class Counters;

    /// Counters - how many times each event happened
    extern Counters global_counters;

    enum class Level
    {
        Global = 0,
        User,
        Process,
        Thread,
        Snapshot
    };

    class Counters
    {
        Counter * counters = nullptr;
        std::unique_ptr<Counter[]> counters_holder;
        /// Used to propagate increments
        Counters * parent = nullptr;

    public:

        Level level = Level::Thread;

        /// By default, any instance have to increment global counters
        Counters(Level level = Level::Thread, Counters * parent = &global_counters);

        /// Global level static initializer
        Counters(Counter * allocated_counters)
            :  counters(allocated_counters), parent(nullptr), level(Level::Global) {}

        inline Counter & operator[] (Event event)
        {
            return counters[event];
        }

        inline void increment(Event event, Count amount = 1)
        {
            Counters * current = this;
            do
            {
                current->counters[event].fetch_add(amount, std::memory_order_relaxed);
                current = current->parent;
            } while (current != nullptr);
        }

        Counters getPartiallyAtomicSnapshot() const;

        /// Reset metrics and parent
        void reset();

        /// Get parent (thread unsafe)
        Counters * getParent()
        {
            return parent;
        }

        /// Set parent (thread unsafe)
        void setParent(Counters * parent_)
        {
            parent = parent_;
        }

        /// Set all counters to zero
        void resetCounters();

        /// Dumps profile events to two column Array(String) and Array(UInt64)
        void dumpToArrayColumns(DB::IColumn * column_names, DB::IColumn * column_value, bool nonzero_only = true);

        static const Event num_counters;
    };

    /// Increment a counter for event. Thread-safe.
    void increment(Event event, Count amount = 1);

    /// Get text description of event by identifier. Returns statically allocated string.
    const char * getDescription(Event event);

    /// Get index just after last event identifier.
    Event end();
}
