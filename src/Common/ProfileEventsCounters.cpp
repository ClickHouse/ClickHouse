#include <Common/LoggingFormatStringHelpers.h>
#include <Common/StackTrace.h>
#include <Common/thread_local_rng.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfileEventsStorage.h>
#include <Common/PerCPU.h>
#include <Common/CurrentThread.h>
#include <Common/TraceSender.h>
#include <Interpreters/Context.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/NamePrompter.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

#include <cfloat>
#include <random>
#include <ranges>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SERVER_OVERLOADED;
}

namespace ProfileEvents
{
    extern const Event OSCPUWaitMicroseconds;
    extern const Event OSCPUVirtualTimeMicroseconds;

/// Cell count for a layout: `cpus` padded rows, or a compact single row of raw events.
ALWAYS_INLINE inline size_t cellCount(uint32_t cpus)
{
    return cpus ? static_cast<size_t>(cpus) * per_cpu_stride : static_cast<size_t>(Counters::num_counters);
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
    , should_trace_array(src.should_trace_array.exchange(nullptr, std::memory_order_acquire))
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
        /// `cpu * per_cpu_stride` is the same at every level, so the parent loops compute it once.
        const size_t row_offset = (cpu >= 0 && static_cast<uint32_t>(cpu) < rows) ? static_cast<size_t>(cpu) * per_cpu_stride : 0;
        std::atomic_ref<Count>(counters[row_offset + event]).fetch_add(amount, std::memory_order_relaxed);
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
    /// Every counter is overwritten right below, so zeroing the 12 KB first is pure waste. A query log
    /// element carries a snapshot and is copied on every logged query.
    : counters_holder(std::make_unique_for_overwrite<Count[]>(num_counters))
{
    std::copy(other.counters_holder.get(), other.counters_holder.get() + num_counters, counters_holder.get());
}

Counters::Snapshot & Counters::Snapshot::operator=(const Snapshot & other)
{
    if (this == &other)
        return *this;

    if (!counters_holder)
        counters_holder = std::make_unique_for_overwrite<Count[]>(num_counters);

    std::copy(other.counters_holder.get(), other.counters_holder.get() + num_counters, counters_holder.get());
    return *this;
}

Counters::Snapshot Counters::getPartiallyAtomicSnapshot() const
{
    Snapshot res;
    for (Event i = Event(0); i < num_counters; ++i)
        res.counters_holder[i] = load(i);
    return res;
}

static const std::unordered_map<std::string_view, Event> & getNameToEventMap()
{
    static const std::unordered_map<std::string_view, Event> map = []
    {
        std::unordered_map<std::string_view, Event> res;
        res.reserve(Counters::num_counters);
        for (Event event = Event(0); event < Counters::num_counters; ++event)
            res.emplace(getName(event), event);
        return res;
    }();
    return map;
}

/// Get ProfileEvent by its name
Event getByName(std::string_view name)
{
    const auto & map = getNameToEventMap();
    auto it = map.find(name);
    if (it == map.end())
    {
        DB::VectorWithMemoryTracking<String> all_names;
        all_names.reserve(Counters::num_counters);
        for (Event event = Event(0); event < Counters::num_counters; ++event)
            all_names.emplace_back(getName(event));

        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown profile event: {}{}",
            name, DB::getHintsErrorMessageSuffix(DB::NamePrompter<3>::getHints(String(name), all_names)));
    }

    return it->second;
}

std::optional<Event> tryGetByName(std::string_view name)
{
    const auto & map = getNameToEventMap();
    auto it = map.find(name);
    if (it == map.end())
        return std::nullopt;
    return it->second;
}

void Counters::setTraceProfileEvent(Event event)
{
    auto * trace_array = should_trace_array.load(std::memory_order_acquire);
    if (!trace_array)
    {
        /// It is very unlikely that it will be allocated twice, since we set it at the beginning of the query
        auto fresh = std::make_unique<std::atomic_bool[]>(num_counters);
        std::atomic_bool * expected = nullptr;
        if (should_trace_array.compare_exchange_strong(expected, fresh.get(), std::memory_order_release, std::memory_order_acquire))
        {
            should_trace_holder = std::move(fresh);
            trace_array = should_trace_holder.get();
        }
        else
            trace_array = expected;
    }
    trace_array[event].store(true, std::memory_order_relaxed);
}

/// A range adaptor that applies `trimWhitespace` to every element,
/// e.g. `std::views::split(list, ',') | trimWhitespaceTransform`.
/// It is kept local to this file on purpose: `Common/StringUtils.h` is directly included by more than
/// two hundred translation units, and exporting this adaptor from there would pull `<ranges>` into all of them.
static constexpr auto trimWhitespaceTransform = std::views::transform([](auto && token)
{
    return trimWhitespace(std::string_view(token.begin(), token.end()));
});

void Counters::setTraceProfileEvents(const String & events_list)
{
    /// The list is written by a human, so allow spaces around the names and a trailing comma.
    bool has_any = false;
    for (const auto name : std::views::split(std::string_view(events_list), ',') | trimWhitespaceTransform)
    {
        if (name.empty())
            continue;

        setTraceProfileEvent(getByName(name));
        has_any = true;
    }

    /// An empty list means "trace everything" - keep this behaviour when the list contains only separators and spaces.
    if (!has_any)
        setTraceAllProfileEvents();
}

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
    Int64 curr_cpu_wait_microseconds = static_cast<Int64>(load(ProfileEvents::OSCPUWaitMicroseconds));
    Int64 curr_cpu_virtual_time_microseconds = static_cast<Int64>(load(ProfileEvents::OSCPUVirtualTimeMicroseconds));

    Int64 os_cpu_wait_microseconds = curr_cpu_wait_microseconds - static_cast<Int64>(prev_cpu_wait_microseconds.load(std::memory_order_acquire));
    Int64 os_cpu_virtual_time_microseconds = curr_cpu_virtual_time_microseconds - static_cast<Int64>(prev_cpu_virtual_time_microseconds.load(std::memory_order_acquire));

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
        /// Small optimization for quite a hot path.
        /// Load with relaxed as it almost always returns null.
        /// If non-null, add an acquire fence.
        if (current->should_trace_array.load(std::memory_order_relaxed))
        {
            if (auto * trace_arr = current->should_trace_array.load(std::memory_order_acquire))
                send_to_trace_log |= trace_arr[event].load(std::memory_order_relaxed);
        }
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
