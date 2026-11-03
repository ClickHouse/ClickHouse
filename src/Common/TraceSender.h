#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include <Common/PipeFDs.h>
#include <Common/ProfileEvents.h>
#include <Common/VariableContext.h>
#include <base/types.h>

class StackTrace;

namespace DB
{

class TraceCollector;

enum class TraceType : uint8_t
{
    Real,
    CPU,
    Memory,
    MemorySample,
    MemoryPeak,
    ProfileEvent,
    JemallocSample,
    MemoryAllocatedWithoutCheck,
    Instrumentation,
    MemoryLargeAllocation
};

/// This is the second part of TraceCollector, that sends stacktrace to the pipe.
/// It has been split out to avoid dependency from interpreters part.
class TraceSender
{
public:
    static constexpr Int8 MEMORY_CONTEXT_UNKNOWN = -1;

    struct Extras
    {
        /// size, ptr - for memory tracing is the amount of memory allocated; for other trace types it is 0.
        Int64 size{};
        void * ptr = nullptr;
        std::optional<VariableContext> memory_context{};
        std::optional<VariableContext> memory_blocked_context{};
        /// Event type and increment for 'ProfileEvent' trace type; for other trace types defaults.
        ProfileEvents::Event event{ProfileEvents::end()};
        ProfileEvents::Count increment{};
    };

    /// Collect a stack trace. This method is signal safe.
    /// Precondition: the TraceCollector object must be created.
    static void send(TraceType trace_type, const StackTrace & stack_trace, Extras extras) noexcept;

    /// True when a TraceCollector is running, i.e. when `send()` can deliver rather than discard.
    /// `shutdown` is checked first, so inside an `in_flight` section the fd is not read while the destructor resets it.
    static bool isCollecting() { return !shutdown.load() && pipe.fds_rw[1] >= 0; }

private:
    friend class TraceCollector;
    static LazyPipeFDs pipe;

    /// `TraceCollector::flush` numbers the markers it writes to the pipe, the collector counts the ones it has read.
    /// Static like `pipe`, because `SYSTEM FLUSH LOGS` can run while `Context::shutdown` destroys the collector.
    static std::mutex flush_mutex;
    static std::condition_variable flush_cv;
    static UInt64 flush_requested;
    static UInt64 flush_completed;

    /// Coordinate shutdown with concurrent `send()` callers (which may run from
    /// profiler signal handlers on any thread). `TraceCollector::~TraceCollector`
    /// sets `shutdown = true` and waits for `in_flight` to reach zero before
    /// closing the pipe, so `close()` cannot race with any in-progress `write()`.
    static std::atomic<bool> shutdown;
    static std::atomic<int> in_flight;

    /// `send()` runs from profiler signal handlers, which means every atomic op it
    /// performs must be lock-free — otherwise libatomic's hidden mutex can deadlock
    /// when a signal interrupts a thread that already holds it. Enforce at compile
    /// time, so a non-lock-free target fails the build instead of silently breaking
    /// signal-safety at runtime.
    static_assert(decltype(TraceSender::shutdown)::is_always_lock_free);
    static_assert(decltype(TraceSender::in_flight)::is_always_lock_free);
};

}
