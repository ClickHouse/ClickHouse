#pragma once

#include <Common/VariableContext.h>
#include <Common/Stopwatch.h>
#include <base/types.h>
#include <base/strong_typedef.h>
#include <Poco/Message.h>
#include <atomic>
#include <memory>
#include <cstddef>


/** Implements global counters for various events happening in the application
  *  - for high level profiling.
  * See .cpp for list of events.
  */

namespace ProfileEvents
{
    /// Event identifier (index in array).
    using Event = StrongTypedef<size_t, struct EventTag>;
    using Count = size_t;
    using Increment = Int64;
    using Counter = std::atomic<Count>;
    class Counters;

    /// Counters - how many times each event happened
    extern Counters global_counters;

    class Timer
    {
    public:
        enum class Resolution : UInt32
        {
            Nanoseconds = 1,
            Microseconds = 1000,
            Milliseconds = 1000000,
        };
        Timer(Counters & counters_, Event timer_event_, Resolution resolution_);
        Timer(Counters & counters_, Event timer_event_, Event counter_event, Resolution resolution_);
        Timer(Timer && other) noexcept
            : counters(other.counters), timer_event(std::move(other.timer_event)), watch(std::move(other.watch)), resolution(std::move(other.resolution))
            {}
        ~Timer() { end(); }
        void cancel() { watch.reset(); }
        void restart() { watch.restart(); }
        void end();
        UInt64 get();

    private:
        Counters & counters;
        Event timer_event;
        Stopwatch watch;
        Resolution resolution;
    };

    class Counters
    {
    private:
        Counter * counters = nullptr;
        std::unique_ptr<Counter[]> counters_holder;
        /// Used to propagate increments
        Counters * parent = nullptr;
        bool trace_profile_events = false;

    public:

        VariableContext level = VariableContext::Thread;

        /// By default, any instance have to increment global counters
        explicit Counters(VariableContext level_ = VariableContext::Thread, Counters * parent_ = &global_counters);

        /// Global level static initializer
        explicit Counters(Counter * allocated_counters) noexcept
            : counters(allocated_counters), parent(nullptr), level(VariableContext::Global) {}

        Counter & operator[] (Event event)
        {
            return counters[event];
        }

        const Counter & operator[] (Event event) const
        {
            return counters[event];
        }

        void increment(Event event, Count amount = 1);
        void incrementNoTrace(Event event, Count amount = 1);

        struct Snapshot
        {
            Snapshot();
            Snapshot(Snapshot &&) = default;

            Count operator[] (Event event) const noexcept
            {
                return counters_holder[event];
            }

            Snapshot & operator=(Snapshot &&) = default;
        private:
            std::unique_ptr<Count[]> counters_holder;

            friend class Counters;
            friend struct CountersIncrement;
        };

        /// Every single value is fetched atomically, but not all values as a whole.
        Snapshot getPartiallyAtomicSnapshot() const;

        /// Reset all counters to zero and reset parent.
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

        void setTraceProfileEvents(bool value)
        {
            trace_profile_events = value;
        }

        /// Set all counters to zero
        void resetCounters();

        /// Add elapsed time to `timer_event` when returned object goes out of scope.
        /// Use the template parameter to control timer resolution, the default
        /// is `Timer::Resolution::Microseconds`.
        template <Timer::Resolution resolution = Timer::Resolution::Microseconds>
        Timer timer(Event timer_event)
        {
            return Timer(*this, timer_event, resolution);
        }

        /// Increment `counter_event` and add elapsed time to `timer_event` when returned object goes out of scope.
        /// Use the template parameter to control timer resolution, the default
        /// is `Timer::Resolution::Microseconds`.
        template <Timer::Resolution resolution = Timer::Resolution::Microseconds>
        Timer timer(Event timer_event, Event counter_event)
        {
            return Timer(*this, timer_event, counter_event, resolution);
        }

        static const Event num_counters;
    };

    enum class ValueType : uint8_t
    {
        Number,
        Bytes,
        Milliseconds,
        Microseconds,
        Nanoseconds,
    };

    /// Increment a counter for event. Thread-safe.
    void increment(Event event, Count amount = 1);

    /// The same as above but ignores value of setting 'trace_profile_events'
    /// and never sends profile event to trace log.
    void incrementNoTrace(Event event, Count amount = 1);

    /// Increment a counter for log messages.
    void incrementForLogMessage(Poco::Message::Priority priority);

    /// Get name of event by identifier. Returns statically allocated string.
    const char * getName(Event event);

    /// Get description of event by identifier. Returns statically allocated string.
    const char * getDocumentation(Event event);

    /// Get value type of event by identifier. Returns enum value.
    ValueType getValueType(Event event);

    /// Get index just after last event identifier.
    Event end();

    struct CountersIncrement
    {
        CountersIncrement() noexcept = default;
        explicit CountersIncrement(Counters::Snapshot const & snapshot);
        CountersIncrement(Counters::Snapshot const & after, Counters::Snapshot const & before);

        CountersIncrement(CountersIncrement &&) = default;
        CountersIncrement & operator=(CountersIncrement &&) = default;

        Increment operator[](Event event) const noexcept
        {
            return increment_holder[event];
        }
    private:
        void init();

        static_assert(sizeof(Count) == sizeof(Increment), "Sizes of counter and increment differ");

        std::unique_ptr<Increment[]> increment_holder;
    };
}
