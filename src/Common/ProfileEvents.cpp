#include <Common/LoggingFormatStringHelpers.h>
#include <Common/StackTrace.h>
#include <Common/thread_local_rng.h>
#include <Common/ProfileEvents.h>
#include <Common/PerCPU.h>
#include <Common/CurrentThread.h>
#include <Common/TraceSender.h>
#include <Interpreters/Context.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/iter_find.hpp>

#include <cfloat>
#include <random>

// clang-format off
/// Available events. Add something here as you wish.
/// If the event is generic (i.e. not server specific)
/// it should be also added to src/Coordination/KeeperConstant.cpp
#include <Common/ProfileEventsList.inc>

#ifdef APPLY_FOR_EXTERNAL_EVENTS
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M) APPLY_FOR_EXTERNAL_EVENTS(M)
#else
    #define APPLY_FOR_EVENTS(M) APPLY_FOR_BUILTIN_EVENTS(M)
#endif

namespace DB::ErrorCodes
{
    extern const int SERVER_OVERLOADED;
}

namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION, VALUE_TYPE) extern const Event NAME = Event(__COUNTER__);
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = Event(__COUNTER__);

/// Row stride padded so each per-CPU row ends on a cache-line boundary. Without this the last
/// few events of one row share a cache line with the first events of the next row, causing
/// false sharing across CPUs at row boundaries.
constexpr size_t counts_per_cache_line = DB::CH_CACHE_LINE_SIZE / sizeof(Count);
static_assert((counts_per_cache_line & (counts_per_cache_line - 1)) == 0);
constexpr size_t per_cpu_stride = (static_cast<size_t>(END) + counts_per_cache_line - 1) & ~(counts_per_cache_line - 1);

/// Cell count for a layout: `cpus` padded rows, or a compact single row of raw events.
ALWAYS_INLINE inline size_t cellCount(uint32_t cpus)
{
    return cpus ? static_cast<size_t>(cpus) * per_cpu_stride : static_cast<size_t>(END);
}

/// Cells are plain `Count` accessed via `std::atomic_ref`; over a suitably aligned object it is
/// lock-free and generates the same code as a `std::atomic` member (required_alignment == 8).
ALWAYS_INLINE inline std::atomic_ref<Count> cell(Count * counters, size_t cpu, Event event)
{
    return std::atomic_ref<Count>(counters[cpu * per_cpu_stride + event]);
}

ALWAYS_INLINE inline AlignedCounters allocateCounters(size_t n)
{
    return AlignedCounters(new (std::align_val_t{DB::CH_CACHE_LINE_SIZE}) Count[n] {});
}

/// Per-CPU storage for `global_counters`, cache-line aligned. `Count` is trivially default-
/// constructible, so this static array is a guaranteed zero-init BSS with no dynamic initializer:
/// valid before any dynamic initializer can touch `global_counters` (which points here), and only
/// touched rows fault in. An `atomic` element is not trivially constructible and would reintroduce
/// dynamic initialization for an array this large — hence plain `Count` cells + `atomic_ref`.
static_assert(std::is_trivially_default_constructible_v<Count>);
alignas(DB::CH_CACHE_LINE_SIZE) static Count global_counters_storage[PerCPU::MAX_CPUS * per_cpu_stride];

/// `cpus` starts at 0 (single-row layout); `ProfileEventsPerCPUInitializer` flips it to
/// `PerCPU::getNumCPUs()` before any worker thread exists.
constexpr Counters::Counters(Count * allocated_counters) noexcept
    : counters(allocated_counters)
    , parent(nullptr)
    , level(VariableContext::Global)
{}

constinit Counters global_counters(global_counters_storage);

/// Per-CPU width applied to newly-created `User`-level `Counters`. Set to `getNumCPUs()` during
/// dynamic static init; `setUserPerCPUEnabled(false)` resets it to 0 to force the compact single-row
/// layout. Read once per `User` ctor; the server sets it at startup before any user exists.
constinit std::atomic<uint32_t> user_counters_cpus = 0;

/// Switches `global_counters` to per-CPU layout during dynamic static init. Only `cpus` is
/// mutated — storage stays put, so both pre- and post-flip readers see a valid view of the same
/// BSS array (pre-flip increments land in row 0, which post-flip sums include). The store is
/// relaxed-atomic because a thread spawned by an earlier TU's dynamic initializer may already be
/// incrementing counters while this runs.
struct ProfileEventsPerCPUInitializer
{
    ProfileEventsPerCPUInitializer()
    {
        const uint32_t cpus = PerCPU::getNumCPUs();
        global_counters.cpus.store(cpus, std::memory_order_relaxed);
        user_counters_cpus.store(cpus, std::memory_order_relaxed);
    }
};
static const ProfileEventsPerCPUInitializer profile_events_per_cpu_initializer;

void setUserPerCPUEnabled(bool enabled)
{
    user_counters_cpus.store(enabled ? PerCPU::getNumCPUs() : 0, std::memory_order_relaxed);
}

const Event Counters::num_counters = END;


Timer::Timer(Counters & counters_, Event timer_event_, Resolution resolution_)
    : counters(counters_), timer_event(timer_event_), resolution(resolution_)
{
}

Timer::Timer(Counters & counters_, Event timer_event_, Event counter_event, Resolution resolution_)
    : Timer(counters_, timer_event_, resolution_)
{
    counters.increment(counter_event);
}

UInt64 Timer::get()
{
    return watch.elapsedNanoseconds() / static_cast<UInt64>(resolution);
}

void Timer::end()
{
    counters.increment(timer_event, get());
    watch.reset();
}

Counters::Counters(VariableContext level_, Counters * parent_)
    /// `User`-level instances snapshot `user_counters_cpus` (stable post-init, server-tunable);
    /// other levels stay single-row (`cpus == 0`). `cpus` is read once and the allocation is
    /// sized from it, so the layout and the row count cannot disagree.
    : cpus(level_ == VariableContext::User ? user_counters_cpus.load(std::memory_order_relaxed) : 0)
    , counters_holder(allocateCounters(cellCount(cpus.load(std::memory_order_relaxed))))
    , parent(parent_)
    , level(level_)
{
    counters = counters_holder.get();
}

Counters::Counters(Counters && src) noexcept
    : counters(std::exchange(src.counters, nullptr))
    , cpus(src.cpus.exchange(0, std::memory_order_relaxed))
    , counters_holder(std::move(src.counters_holder))
    , parent(src.parent.exchange(nullptr, std::memory_order_acquire))
    , should_trace_array(src.should_trace_array.exchange(nullptr, std::memory_order_relaxed))
    , should_trace_holder(std::move(src.should_trace_holder))
    , trace_all_profile_events(src.trace_all_profile_events.load(std::memory_order_relaxed))
    , level(src.level)
{
}

void Counters::resetCounters()
{
    if (!counters)
        return;
    const size_t total = cellCount(cpus.load(std::memory_order_relaxed));
    for (size_t i = 0; i < total; ++i)
        std::atomic_ref<Count>(counters[i]).store(0, std::memory_order_relaxed);
}

Count Counters::load(Event event) const
{
    const uint32_t rows = cpus.load(std::memory_order_relaxed);
    if (!rows)
        return cell(counters, 0, event).load(std::memory_order_relaxed);
    Count sum = 0;
    for (uint32_t s = 0; s < rows; ++s)
        sum += cell(counters, s, event).load(std::memory_order_relaxed);
    return sum;
}

void Counters::setParent(Counters * parent_)
{
    parent.store(parent_, std::memory_order_release);
}

void Counters::setUserCounters(Counters * user)
{
    auto * current_val = this;
    auto * parent_val = this->parent.load(std::memory_order_acquire);

    while (parent_val != nullptr && parent_val->level != VariableContext::Global && parent_val->level != VariableContext::User)
    {
        current_val = parent_val;
        parent_val = current_val->parent.load(std::memory_order_acquire);
    }

    current_val->parent.store(user, std::memory_order_release);
}

void Counters::setTraceAllProfileEvents()
{
    trace_all_profile_events.store(true, std::memory_order_relaxed);
}

void Counters::fetchAdd(Event event, Count amount, int32_t cpu)
{
    const uint32_t rows = cpus.load(std::memory_order_relaxed);
    if (rows)
    {
        /// `cpu` may be >= rows if a CPU above `MAX_CPUS` is online, and -1 on error. In both
        /// cases, fall back to row 0 — still atomic, still correct, just with less cache locality.
        const size_t row = (cpu >= 0 && static_cast<uint32_t>(cpu) < rows) ? static_cast<size_t>(cpu) : 0;
        cell(counters, row, event).fetch_add(amount, std::memory_order_relaxed);
    }
    else
        cell(counters, 0, event).fetch_add(amount, std::memory_order_relaxed);
}

void Counters::reset()
{
    setParent(nullptr);
    resetCounters();
}

Counters::Snapshot::Snapshot()
    : counters_holder(new Count[num_counters] {})
{}

Counters::Snapshot::Snapshot(const Snapshot & other)
    : counters_holder(new Count[num_counters] {})
{
    std::copy(other.counters_holder.get(), other.counters_holder.get() + num_counters, counters_holder.get());
}

Counters::Snapshot & Counters::Snapshot::operator=(const Snapshot & other)
{
    Snapshot tmp(other);
    counters_holder = std::move(tmp.counters_holder);
    return *this;
}

Counters::Snapshot Counters::getPartiallyAtomicSnapshot() const
{
    Snapshot res;
    for (Event i = Event(0); i < num_counters; ++i)
        res.counters_holder[i] = load(i);
    return res;
}

static const std::array<std::string_view, END> names =
{
#define M(NAME, DOCUMENTATION, VALUE_TYPE) #NAME,
    APPLY_FOR_EVENTS(M)
#undef M
};

const std::string_view & getName(Event event)
{
    return names[event];
}

static const std::array<std::string_view, END> docs =
{
#define M(NAME, DOCUMENTATION, VALUE_TYPE) DOCUMENTATION,
    APPLY_FOR_EVENTS(M)
#undef M
};

const std::string_view & getDocumentation(Event event)
{
    return docs[event];
}

/// Get ProfileEvent by its name
Event getByName(std::string_view name)
{
    static std::unordered_map<std::string_view, Event> map =
    {
#define M(NAME, DOCUMENTATION, VALUE_TYPE) {#NAME, ProfileEvents::NAME},
        APPLY_FOR_EVENTS(M)
#undef M
    };

    return map.at(name);
}

void Counters::setTraceProfileEvent(Event event)
{
    auto * trace_array = should_trace_array.load(std::memory_order_relaxed);
    if (!trace_array)
    {
        /// It is very unlikely that it will be allocated twice, since we set it at the beginning of the query
        auto fresh = std::make_unique<std::atomic_bool[]>(num_counters);
        std::atomic_bool * expected = nullptr;
        if (should_trace_array.compare_exchange_strong(expected, fresh.get(), std::memory_order_release, std::memory_order_relaxed))
        {
            should_trace_holder = std::move(fresh);
            trace_array = should_trace_holder.get();
        }
        else
            trace_array = expected;
    }
    trace_array[event].store(true, std::memory_order_relaxed);
}

void Counters::setTraceProfileEvents(const String & events_list)
{
    for (auto it = boost::make_split_iterator(events_list, boost::first_finder(",", boost::is_equal()));
        it != decltype(it)();
        ++it)
    {
        setTraceProfileEvent(getByName(std::string_view(*it)));
    }
}


ValueType getValueType(Event event)
{
    static ValueType strings[] =
    {
    #define M(NAME, DOCUMENTATION, VALUE_TYPE) VALUE_TYPE,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

Event end() { return END; }

bool checkCPUOverload(Int64 os_cpu_busy_time_threshold, double min_ratio, double max_ratio, bool should_throw)
{
    if ((max_ratio <= 0.0) || (max_ratio <= min_ratio))
        return false;
    double cpu_load = global_counters.getCPUOverload(os_cpu_busy_time_threshold);

    if (cpu_load > DBL_EPSILON)
    {
        double current_ratio = std::min(std::max(min_ratio, cpu_load), max_ratio);
        double probability_to_throw = (max_ratio <= min_ratio) ? 0.0 : (current_ratio - min_ratio) / (max_ratio - min_ratio);

        const PreformattedMessage error_message = PreformattedMessage::create("CPU is overloaded, CPU is waiting for execution way more than executing, "
                "ratio of wait time (OSCPUWaitMicroseconds metric) to busy time (OSCPUVirtualTimeMicroseconds metric) is {}. "
                "Min ratio for error {}{}, max ratio for error {}{}, probability used to decide whether to {} {}.{}",
                current_ratio,
                should_throw ? "(min_os_cpu_wait_time_ratio_to_throw setting) " : "",
                min_ratio,
                should_throw ? "(max_os_cpu_wait_time_ratio_to_throw setting) " : "",
                max_ratio,
                should_throw ? "discard the query" : "drop the connection",
                probability_to_throw,
                should_throw ? " Consider reducing the number of queries or increase backoff between retries." : "");

        if (std::bernoulli_distribution server_overloaded(probability_to_throw); server_overloaded(thread_local_rng))
        {
            if (should_throw)
                throw DB::Exception(error_message, DB::ErrorCodes::SERVER_OVERLOADED);
            else
            {
                LOG_ERROR(getLogger("ProfileEvents"), error_message);
                return true;
            }
        }
    }

    return false;
}

void increment(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().increment(event, amount);
}

void incrementNoTrace(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().incrementNoTrace(event, amount);
}

void incrementSignalSafe(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().incrementSignalSafe(event, amount);
}

double Counters::getCPUOverload(Int64 os_cpu_busy_time_threshold, bool reset)
{
    /// It's possible that we'll have slightly inconsistent values between wait time and busy time. But since we take the value of CPU wait time first,
    /// it should not affect the situation a lot. In the worst case scenario we will have a slightly lower CPU overload value than it should be, but it's fine.
    Int64 curr_cpu_wait_microseconds = static_cast<Int64>(load(OSCPUWaitMicroseconds));
    Int64 curr_cpu_virtual_time_microseconds = static_cast<Int64>(load(OSCPUVirtualTimeMicroseconds));

    Int64 os_cpu_wait_microseconds = curr_cpu_wait_microseconds - prev_cpu_wait_microseconds.load(std::memory_order_acquire);
    Int64 os_cpu_virtual_time_microseconds = curr_cpu_virtual_time_microseconds - prev_cpu_virtual_time_microseconds.load(std::memory_order_acquire);

    if (reset)
    {
        /// It's important to update wait time first, since the atomicity is not guaranteed for both counters at the same time.
        /// So in the worst case scenario, we'll update prev wait time first, which will result in an underestimated wait time and lower CPU overload value.
        prev_cpu_wait_microseconds.store(curr_cpu_wait_microseconds, std::memory_order_release);
        prev_cpu_virtual_time_microseconds.store(curr_cpu_virtual_time_microseconds, std::memory_order_release);
    }

    if (os_cpu_virtual_time_microseconds <= os_cpu_busy_time_threshold || os_cpu_wait_microseconds <= 0)
        return 0;

    return static_cast<double>(os_cpu_wait_microseconds) / static_cast<double>(os_cpu_virtual_time_microseconds);
}

void Counters::increment(Event event, Count amount)
{
    Counters * current = this;
    bool send_to_trace_log = false;
    const int32_t cpu = PerCPU::getCurrentCPU();

    do
    {
        current->fetchAdd(event, amount, cpu);
        if (auto * trace_arr = current->should_trace_array.load(std::memory_order_relaxed))
            send_to_trace_log |= trace_arr[event].load(std::memory_order_relaxed);
        send_to_trace_log |= current->trace_all_profile_events.load(std::memory_order_relaxed);

        current = current->parent.load(std::memory_order_acquire);
    } while (current != nullptr);

    if (unlikely(send_to_trace_log))
        DB::TraceSender::send(DB::TraceType::ProfileEvent, StackTrace(), {.event = event, .increment = amount});
}

void Counters::incrementNoTrace(Event event, Count amount)
{
    Counters * current = this;
    const int32_t cpu = PerCPU::getCurrentCPU();
    do
    {
        current->fetchAdd(event, amount, cpu);
        current = current->parent.load(std::memory_order_acquire);
    } while (current != nullptr);
}

void Counters::incrementSignalSafe(Event event, Count amount)
{
    static_assert(std::atomic_ref<Count>::is_always_lock_free);

    Counters * current = this;
    /// Must stay async-signal-safe (called from signal/crash handlers), so unlike `incrementNoTrace`
    /// it does not call `sched_getcpu`; `cpu = -1` routes every level to its row 0.
    do
    {
        current->fetchAdd(event, amount, -1);
        current = current->parent.load(std::memory_order_acquire);
    } while (current != nullptr);
}

CountersIncrement::CountersIncrement(Counters::Snapshot const & snapshot)
{
    init();
    memcpy(increment_holder.get(), snapshot.counters_holder.get(), Counters::num_counters * sizeof(Increment));
}

/// NO_SANITIZE_UNDEFINED - Hardware perf event counters can overflow, prevent exception in ubsan build
NO_SANITIZE_UNDEFINED CountersIncrement::CountersIncrement(Counters::Snapshot const & after, Counters::Snapshot const & before)
{
    init();
    for (Event i = Event(0); i < Counters::num_counters; ++i)
        increment_holder[i] = static_cast<Increment>(after[i]) - static_cast<Increment>(before[i]);
}

void CountersIncrement::init()
{
    increment_holder = std::make_unique<Increment[]>(Counters::num_counters);
}

}

#undef APPLY_FOR_EVENTS
