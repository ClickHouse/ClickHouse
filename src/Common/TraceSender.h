#pragma once

#include <atomic>
#include <string_view>

#include <Common/PipeFDs.h>
#include <Common/ProfileEvents.h>
#include <Common/VariableContext.h>
#include <Common/setThreadName.h>
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

    /// Normally query_id is a UUID (string with a fixed length) but user can provide custom query_id.
    /// Thus upper bound on query_id length should be introduced to avoid buffer overflow in signal handler.
    ///
    /// And it cannot be large, since otherwise it will not fit into PIPE_BUF.
    /// The performance test query ids can be surprisingly long like
    /// `aggregating_merge_tree_simple_aggregate_function_string.query100.profile100`,
    /// so make some allowance for them as well.
    static constexpr size_t QUERY_ID_MAX_LEN = 95;

    /// The thread a trace is attributed to. `send` fills it from the calling thread; a sampler that
    /// captures another thread's stack (the macOS query profiler) passes the sampled thread's instead.
    struct Origin
    {
        std::string_view query_id;
        UInt64 thread_id = 0;
        ThreadName thread_name = ThreadName::UNKNOWN;
    };

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
    static void send(TraceType trace_type, const StackTrace & stack_trace, Extras extras, Origin origin) noexcept;

    /// True when a TraceCollector is running, i.e. when `send()` can deliver rather than discard.
    static bool isCollecting() { return pipe.fds_rw[1] >= 0 && !shutdown.load(); }

private:
    friend class TraceCollector;
    static LazyPipeFDs pipe;

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
