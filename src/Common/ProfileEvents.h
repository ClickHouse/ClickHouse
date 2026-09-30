#pragma once

#include <Common/VariableContext.h>
#include <Common/Stopwatch.h>
#include <Common/CacheLine.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <base/strong_typedef.h>
#include <atomic>
#include <memory>
#include <new>
#include <cstddef>


/** Implements global counters for various events happening in the application
  *  - for high level profiling.
  * See .cpp for list of events.
  */

namespace ProfileEvents
{
    /// Event identifier (index in array).
    using Event = StrongTypedef<size_t, struct EventTag>;
    /// Not `size_t`: counters are 64-bit even on 32-bit platforms, and must match `Increment`.
    using Count = UInt64;
    using Increment = Int64;

    class Counters;
    class NonAllocatingEvent;

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
        struct CounterRow;
        /// Every level uses the same hot/cold row. User/global counters shard rows by CPU.
        CounterRow * counters = nullptr;
        std::atomic<uint32_t> cpus = 0;
        std::unique_ptr<CounterRow[]> counters_holder;
        /// Borrowed, process-lifetime backing, usable before dynamic initialization.
        static CounterRow global_storage[];
        static std::unique_ptr<CounterRow[]> allocateRows(uint32_t rows, VariableContext allocation_level);

        /// Used to propagate increments.
        /// Requires acquire-release:
        /// 1. Thread A constructs Counters object and attaches Counters pointer
        ///    (e.g. ProcessList::insert where the user's Counters is constructed right before calling setUserCounters).
        /// 2. Thread B traverses the chain and dereferences each pointer (e.g. another thread in thread group).
        /// 3. If Thread B sees a pointer, it should be guaranteed to see the object's memory without data races.
        ///    Hence, we need the Thread A's pointer store to synchronize-with the Thread B's pointer load.
        std::atomic<Counters *> parent = {};

        std::atomic<Count> prev_cpu_wait_microseconds = 0;
        std::atomic<Count> prev_cpu_virtual_time_microseconds = 0;

        /// Lazily allocated on first setTraceProfileEvent().
        /// The thread which allocates a buffer and updates the should_trace_array pointer
        /// should synchronize-with any thread that reads the pointer and reads from the buffer.
        /// Therefore, should_trace_array requires acquire-release.
        std::atomic<std::atomic_bool *> should_trace_array = nullptr;
        std::unique_ptr<std::atomic_bool[]> should_trace_holder;
        std::atomic_bool trace_all_profile_events = false;

        Count load(Event event) const;
        template <bool allow_allocation>
        void fetchAdd(Event event, Count amount, int32_t cpu);

        template <bool allow_allocation>
        void incrementImpl(Event event, Count amount);

    public:

        VariableContext level = VariableContext::Thread;

        /// By default, any instance have to increment global counters
        explicit Counters(VariableContext level_ = VariableContext::Thread, Counters * parent_ = &global_counters);

        /// constexpr so `global_counters` can be `constinit` — usable before any dynamic init.
        struct GlobalTag {};
        constexpr explicit Counters(GlobalTag) noexcept;

        friend struct ProfileEventsPerCPUInitializer;

        Counters(Counters && src) noexcept;
        ~Counters();

        double getCPUOverload(Int64 os_cpu_busy_time_threshold, bool reset = false);

        Count operator[] (Event event) const { return load(event); }

        void increment(Event event, Count amount = 1);

        /// Reserve an event in every CPU row and parent before entering an allocation-denied scope.
        /// Reset retains reservations; a newly attached parent must be reserved separately.
        void preallocate(Event event);

        /// Publish through already reserved backing, including every current parent.
        /// Missing backing terminates instead of allocating, including in release builds.
        /// Retains ordinary tracing; this is not the signal-safe API.
        void incrementNonAllocating(Event event, Count amount = 1) noexcept;

        /// Statically hot events need no preceding reservation.
        void incrementNonAllocating(NonAllocatingEvent event, Count amount = 1) noexcept;
        void incrementNoTrace(Event event, Count amount = 1);
        void incrementSignalSafe(NonAllocatingEvent event, Count amount = 1);

        struct Snapshot
        {
            Snapshot();
            Snapshot(Snapshot &&) = default;
            Snapshot(const Snapshot & other);

            Count operator[] (Event event) const noexcept
            {
                return counters_holder[event];
            }

            Snapshot & operator=(Snapshot &&) = default;
            Snapshot & operator=(const Snapshot & other);
        private:
            std::unique_ptr<Count[]> counters_holder;

            friend class Counters;
            friend struct CountersIncrement;
        };

        /// Every single value is fetched atomically, but not all values as a whole.
        Snapshot getPartiallyAtomicSnapshot() const;

        /// Reset all counters to zero and reset parent.
        void reset();

        /// Set parent (thread unsafe)
        void setUserCounters(Counters * user);

        /// Set parent (thread unsafe)
        void setParent(Counters * parent_);

        void setTraceAllProfileEvents();

        void setTraceProfileEvent(ProfileEvents::Event event);
        void setTraceProfileEvents(const String & events_list);

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

    /// Enable/disable per-CPU sharding for newly-created `User`-level `Counters` (server-wide).
    void setUserPerCPUEnabled(bool enabled);

    /// Increment a counter for event. Thread-safe.
    void increment(Event event, Count amount = 1);

    /// Reserve backing in the current thread and its parent chain without incrementing the event.
    void preallocate(Event event);

    /// Publish an already reserved event on the current thread's counter chain.
    void incrementNonAllocating(Event event, Count amount = 1) noexcept;

    /// The same as above but ignores value of setting 'trace_profile_events'
    /// and never sends profile event to trace log.
    void incrementNoTrace(Event event, Count amount = 1);

    /// Async-signal-safe variant of `incrementNoTrace` (no `sched_getcpu`). Use ONLY from
    /// signal/crash handlers. The token requires an event with preallocated storage.
    void incrementSignalSafe(NonAllocatingEvent event, Count amount = 1);

    /// Get name of event by identifier. Returns statically allocated string.
    const std::string_view & getName(Event event);

    /// Get description of event by identifier. Returns statically allocated string.
    const std::string_view & getDocumentation(Event event);

    /// Get ProfileEvent by its name
    Event getByName(std::string_view name);

    /// Get value type of event by identifier. Returns enum value.
    ValueType getValueType(Event event);

    /// Get index just after last event identifier.
    Event end();

    /// Check CPU overload. If should_throw parameter is set, the method will throw when the server is overloaded.
    /// Otherwise, this method will return true if the server is overloaded.
    bool checkCPUOverload(Int64 os_cpu_busy_time_threshold, double min_ratio, double max_ratio, bool should_throw);

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
