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
    using Count = size_t;
    using Increment = Int64;

    /// Counter cells are plain `Count`, accessed atomically via `std::atomic_ref`. Keeping the
    /// storage trivially default-constructible is what lets the static `global_counters` backing
    /// array be a guaranteed zero-init BSS with no dynamic initializer (an `atomic` element is not
    /// trivially constructible, which reintroduces dynamic init for a large enough array).
    struct AlignedCountersDeleter
    {
        void operator()(Count * p) const noexcept { ::operator delete[](p, std::align_val_t{DB::CH_CACHE_LINE_SIZE}); }
    };
    using AlignedCounters = std::unique_ptr<Count[], AlignedCountersDeleter>;

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
        /// Per-CPU: `cpus * per_cpu_stride` cells, cell for CPU `c`/event `e` at `c * per_cpu_stride + e`
        /// (stride rounds `num_counters` up to keep rows on separate cache lines). An out-of-range CPU
        /// falls back to row 0. Otherwise just `num_counters` cells indexed by event.
        Count * counters = nullptr;
        /// 0 → no per-CPU. Set once (static init flips it for `global_counters`, ctor for `User`)
        /// and only grows the view over the same zeroed storage, so relaxed loads suffice; atomic
        /// because a thread spawned during another TU's dynamic init may increment concurrently
        /// with the flip.
        std::atomic<uint32_t> cpus = 0;
        AlignedCounters counters_holder;
        /// Used to propagate increments
        std::atomic<Counters *> parent = {};
        std::atomic<Count> prev_cpu_wait_microseconds = 0;
        std::atomic<Count> prev_cpu_virtual_time_microseconds = 0;

        /// Lazily allocated on first setTraceProfileEvent()
        std::atomic<std::atomic_bool *> should_trace_array = nullptr;
        std::unique_ptr<std::atomic_bool[]> should_trace_holder;
        std::atomic_bool trace_all_profile_events = false;

        Count load(Event event) const;
        void fetchAdd(Event event, Count amount, int32_t cpu);

    public:

        VariableContext level = VariableContext::Thread;

        /// By default, any instance have to increment global counters
        explicit Counters(VariableContext level_ = VariableContext::Thread, Counters * parent_ = &global_counters);

        /// constexpr so `global_counters` can be `constinit` — usable before any dynamic init.
        constexpr explicit Counters(Count * allocated_counters) noexcept;

        friend struct ProfileEventsPerCPUInitializer;

        Counters(Counters && src) noexcept;

        double getCPUOverload(Int64 os_cpu_busy_time_threshold, bool reset = false);

        Count operator[] (Event event) const { return load(event); }

        void increment(Event event, Count amount = 1);
        void incrementNoTrace(Event event, Count amount = 1);
        void incrementSignalSafe(Event event, Count amount = 1);

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

    /// The same as above but ignores value of setting 'trace_profile_events'
    /// and never sends profile event to trace log.
    void incrementNoTrace(Event event, Count amount = 1);

    /// Async-signal-safe variant of `incrementNoTrace` (no `sched_getcpu`). Use ONLY from
    /// signal/crash handlers.
    void incrementSignalSafe(Event event, Count amount = 1);

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
