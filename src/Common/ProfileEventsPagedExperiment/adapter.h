#pragma once

#include <Common/ProfileEventsPagedExperiment/hot_paged.h>
#include <Common/ProfileEventsPagedExperiment/catalogue.h>

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace ProfileEvents::PagedExperiment
{
using Count = uint64_t;
using Event = PagedExperimentStorage::Event;
using Paged = PagedExperimentStorage::HotPaged;

enum class Mode : uintptr_t
{
    Dense = 0,
    Paged = 1,
};

/// All fields are unsigned 64-bit native-endian words. External readers must tolerate
/// non-transactional snapshots. The ready word is published after initialization.
struct Diagnostics
{
    uint64_t magic = UINT64_C(0x4350455850455231);
    uint64_t version = 1;
    uint64_t ready = 0;
    uint64_t mode = 0;
    uint64_t hot = 0;
    uint64_t page = 0;
    uint64_t segment = 0;
    uint64_t event_count = PagedExperimentStorage::EventCount;
    uint64_t counters_wrapper_bytes = 0;
    uint64_t created = 0;
    uint64_t destroyed = 0;
    uint64_t live_objects = 0;
    uint64_t peak_live_objects = 0;
    uint64_t initial_requested_sum = 0;
    uint64_t initial_usable_sum = 0;
    uint64_t initial_allocations_sum = 0;
    uint64_t final_requested_sum = 0;
    uint64_t final_usable_sum = 0;
    uint64_t final_allocations_sum = 0;
    uint64_t final_requested_max = 0;
    uint64_t final_usable_max = 0;
    uint64_t final_backend_wrapper_requested_sum = 0;
    uint64_t final_backend_wrapper_usable_sum = 0;
    uint64_t published_cold_allocations_sum = 0;
};
static_assert(std::atomic_ref<uint64_t>::is_always_lock_free);

[[noreturn]] inline void fail(const char * message, size_t size, int code) noexcept
{
    /// Also used by the signal path: no allocator, exception, logger, or mutex.
    const int saved_errno = errno;
    static_cast<void>(::write(STDERR_FILENO, message, size));
    errno = saved_errno;
    ::_exit(code);
}

#define COUNTER_EXPERIMENT_FAIL(message, code) fail(message "\n", sizeof(message "\n") - 1, code)

inline size_t number(const char * name, size_t default_value)
{
    const char * value = std::getenv(name);
    if (!value)
        return default_value;
    size_t result = 0;
    if (!*value)
        COUNTER_EXPERIMENT_FAIL("Counter experiment: empty numeric environment value", 78);
    for (; *value; ++value)
    {
        if (*value < '0' || *value > '9' || result > 65535 / 10)
            COUNTER_EXPERIMENT_FAIL("Counter experiment: invalid numeric environment value", 78);
        result = result * 10 + static_cast<unsigned>(*value - '0');
    }
    return result;
}

inline size_t hotCount()
{
    const size_t value = number("CH_COUNTER_HOT", 128);
    if (!value || value > PagedExperimentStorage::EventCount)
        COUNTER_EXPERIMENT_FAIL("Counter experiment: hot prefix is outside the catalogue", 78);
    return value;
}

struct Configuration
{
    Mode mode = Mode::Dense;
    PagedExperimentStorage::Layout layout;
    size_t page;
    size_t segment;
    Diagnostics * diagnostics = nullptr;

    Configuration()
        : layout(hotCount())
        , page(number("CH_COUNTER_PAGE", 32))
        , segment(0)
    {
        const char * value = std::getenv("CH_COUNTER_STORAGE");
        if (!value || std::strcmp(value, "dense") == 0)
            mode = Mode::Dense;
        else if (std::strcmp(value, "paged") == 0)
            mode = Mode::Paged;
        else
            COUNTER_EXPERIMENT_FAIL("Counter experiment: unknown storage mode", 78);
        const auto valid_geometry = [](size_t size)
        {
            return size == 8 || size == 16 || size == 32 || size == 64 || size == 128;
        };
        if (!valid_geometry(page) || !layout.hot_count)
            COUNTER_EXPERIMENT_FAIL("Counter experiment: invalid geometry or empty hot prefix", 78);

        const char * path = std::getenv("CH_COUNTER_LAYOUT");
        if (mode != Mode::Dense && (!path || path[0] != '/'))
            COUNTER_EXPERIMENT_FAIL("Counter experiment: compact storage requires an absolute layout path", 78);
        if (path)
        {
            const int fd = ::open(path, O_RDONLY | O_CLOEXEC);
            if (fd < 0)
                COUNTER_EXPERIMENT_FAIL("Counter experiment: cannot open layout", 78);
            /// A permutation uses fewer than 16 KiB; reject extra input, do not truncate it.
            char buffer[32768];
            size_t used = 0;
            for (;;)
            {
                if (used == sizeof(buffer))
                    COUNTER_EXPERIMENT_FAIL("Counter experiment: layout file too large", 78);
                const ssize_t bytes = ::read(fd, buffer + used, sizeof(buffer) - used);
                if (bytes < 0 && errno == EINTR)
                    continue;
                if (bytes < 0)
                    COUNTER_EXPERIMENT_FAIL("Counter experiment: cannot read layout", 78);
                if (!bytes)
                    break;
                used += static_cast<size_t>(bytes);
            }
            if (const int result = ::close(fd); result != 0)
                COUNTER_EXPERIMENT_FAIL("Counter experiment: cannot close layout", 78);
            std::array<bool, PagedExperimentStorage::EventCount> seen{};
            size_t position = 0;
            size_t slot = 0;
            while (position < used)
            {
                const auto whitespace = [](char ch)
                {
                    return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t';
                };
                if (whitespace(buffer[position]))
                {
                    ++position;
                    continue;
                }
                size_t event = 0;
                size_t digits = 0;
                while (position < used && !whitespace(buffer[position]))
                {
                    const char ch = buffer[position++];
                    if (ch < '0' || ch > '9' || event >= PagedExperimentStorage::EventCount)
                        COUNTER_EXPERIMENT_FAIL("Counter experiment: malformed layout ID", 78);
                    event = event * 10 + static_cast<unsigned>(ch - '0');
                    ++digits;
                }
                if (!digits || event >= PagedExperimentStorage::EventCount || slot >= PagedExperimentStorage::EventCount || seen[event])
                    COUNTER_EXPERIMENT_FAIL("Counter experiment: invalid or duplicate layout ID", 78);
                seen[event] = true;
                layout.slot_of[event] = static_cast<Event>(slot);
                layout.event_at_slot[slot++] = static_cast<Event>(event);
            }
            if (slot != PagedExperimentStorage::EventCount)
                COUNTER_EXPERIMENT_FAIL("Counter experiment: incomplete layout", 78);
        }
        if (mode != Mode::Dense)
        {
            for (const Event event : requiredHotEvents())
            {
                if (layout.slot_of[event] >= layout.hot_count)
                    COUNTER_EXPERIMENT_FAIL("Counter experiment: required signal or allocation event is cold", 78);
            }
        }
        if (const char * diagnostics_path = std::getenv("CH_COUNTER_DIAGNOSTICS"))
        {
            if (diagnostics_path[0] != '/')
                COUNTER_EXPERIMENT_FAIL("Counter experiment: diagnostics path must be absolute", 78);
            const int fd = ::open(diagnostics_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
            if (fd < 0 || ::ftruncate(fd, sizeof(Diagnostics)) != 0)
                COUNTER_EXPERIMENT_FAIL("Counter experiment: cannot create diagnostics", 78);
            void * memory = ::mmap(nullptr, sizeof(Diagnostics), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (const int result = ::close(fd); result != 0)
                COUNTER_EXPERIMENT_FAIL("Counter experiment: cannot close diagnostics", 78);
            if (memory == MAP_FAILED)
                COUNTER_EXPERIMENT_FAIL("Counter experiment: cannot map diagnostics", 78);
            diagnostics = new (memory) Diagnostics;
            diagnostics->mode = static_cast<uint64_t>(mode);
            diagnostics->hot = layout.hot_count;
            diagnostics->page = page;
            diagnostics->segment = segment;
            std::atomic_ref<uint64_t>(diagnostics->ready).store(1, std::memory_order_release);
        }
    }
};

inline const Configuration & configuration()
{
    /// No heap allocation in this initializer. Process constructors are the only callers
    /// before it is initialized; signal/update/destruction paths never initialize it.
    static const Configuration result;
    return result;
}

inline Mode kind(const Count * pointer) noexcept
{
    return static_cast<Mode>(reinterpret_cast<uintptr_t>(pointer) & 3);
}

template <class Backend>
inline Backend * object(Count * pointer) noexcept
{
    return reinterpret_cast<Backend *>(reinterpret_cast<uintptr_t>(pointer) & ~uintptr_t{3});
}

template <class Backend>
inline const Backend * object(const Count * pointer) noexcept
{
    return reinterpret_cast<const Backend *>(reinterpret_cast<uintptr_t>(pointer) & ~uintptr_t{3});
}

inline Count * create()
{
    const auto & config = configuration();
    if (config.mode == Mode::Paged)
    {
        auto * result = new Paged(config.layout, config.page);
        return reinterpret_cast<Count *>(reinterpret_cast<uintptr_t>(result) | static_cast<uintptr_t>(Mode::Paged));
    }
    COUNTER_EXPERIMENT_FAIL("Counter experiment: invalid compact construction", 78);
}
static_assert(alignof(Paged) >= 4);

inline thread_local unsigned cold_update_depth = 0;
struct ColdUpdateGuard
{
    ColdUpdateGuard()
    {
        if (cold_update_depth)
            COUNTER_EXPERIMENT_FAIL("Counter experiment: recursive cold update is unsupported", 79);
        ++cold_update_depth;
    }
    ~ColdUpdateGuard()
    {
        --cold_update_depth;
    }
};

template <class Backend>
inline void addTo(Backend & backend, Event event, Count amount)
{
    if (!amount || backend.tryIncrementHot(event, amount))
        return;
    ColdUpdateGuard guard;
    backend.increment(event, amount);
}

inline void add(Count * pointer, Event event, Count amount)
{
    addTo(*object<Paged>(pointer), event, amount);
}

inline void addSignalSafe(Count * pointer, Event event, Count amount) noexcept
{
    if (!amount)
        return;
    const bool hot = object<Paged>(pointer)->tryIncrementHot(event, amount);
    if (!hot)
        COUNTER_EXPERIMENT_FAIL("Counter experiment: cold signal event is unsupported", 80);
}

inline Count load(const Count * pointer, Event event)
{
    return object<Paged>(pointer)->load(event);
}

inline void snapshot(const Count * pointer, Count * output)
{
    object<Paged>(pointer)->snapshot(output);
}

inline void reset(Count * pointer)
{
    object<Paged>(pointer)->resetCounters();
}

inline void destroy(Count * pointer) noexcept
{
    delete object<Paged>(pointer);
}

inline void addDiagnostic(uint64_t & destination, uint64_t value)
{
    std::atomic_ref<uint64_t>(destination).fetch_add(value, std::memory_order_relaxed);
}

inline void maximize(uint64_t & destination, uint64_t value)
{
    std::atomic_ref<uint64_t> target(destination);
    uint64_t previous = target.load(std::memory_order_relaxed);
    while (previous < value && !target.compare_exchange_weak(previous, value, std::memory_order_relaxed))
    {
    }
}

inline PagedExperimentStorage::MemoryUsage backing(const Count * pointer)
{
    if (kind(pointer) == Mode::Dense)
        return PagedExperimentStorage::Array<Count>::allocationMemory(pointer, PagedExperimentStorage::EventCount);
    return object<Paged>(pointer)->memory();
}

inline void constructed(const Count * pointer, size_t counters_size)
{
    auto * diagnostics = configuration().diagnostics;
    if (!diagnostics)
        return;
    const auto memory = backing(pointer);
    std::atomic_ref<uint64_t>(diagnostics->counters_wrapper_bytes).store(counters_size, std::memory_order_relaxed);
    addDiagnostic(diagnostics->created, 1);
    const auto live = std::atomic_ref<uint64_t>(diagnostics->live_objects).fetch_add(1, std::memory_order_relaxed) + 1;
    maximize(diagnostics->peak_live_objects, live);
    addDiagnostic(diagnostics->initial_requested_sum, memory.requested);
    addDiagnostic(diagnostics->initial_usable_sum, memory.usable);
    addDiagnostic(diagnostics->initial_allocations_sum, memory.allocations);
}

inline void destroyed(const Count * pointer)
{
    auto * diagnostics = configuration().diagnostics;
    if (!diagnostics)
        return;
    const auto memory = backing(pointer);
    addDiagnostic(diagnostics->destroyed, 1);
    std::atomic_ref<uint64_t>(diagnostics->live_objects).fetch_sub(1, std::memory_order_relaxed);
    addDiagnostic(diagnostics->final_requested_sum, memory.requested);
    addDiagnostic(diagnostics->final_usable_sum, memory.usable);
    addDiagnostic(diagnostics->final_allocations_sum, memory.allocations);
    maximize(diagnostics->final_requested_max, memory.requested);
    maximize(diagnostics->final_usable_max, memory.usable);
    PagedExperimentStorage::MemoryUsage wrapper;
    if (kind(pointer) == Mode::Paged)
        wrapper = PagedExperimentStorage::Array<Paged>::allocationMemory(object<Paged>(pointer), 1);
    addDiagnostic(diagnostics->final_backend_wrapper_requested_sum, wrapper.requested);
    addDiagnostic(diagnostics->final_backend_wrapper_usable_sum, wrapper.usable);
    const size_t base_allocations = kind(pointer) == Mode::Paged
        ? 1 + (configuration().layout.hot_count < PagedExperimentStorage::EventCount) : 1;
    addDiagnostic(diagnostics->published_cold_allocations_sum, memory.allocations - base_allocations);
}

#undef COUNTER_EXPERIMENT_FAIL
}
