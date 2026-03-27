#pragma once

#include <base/defines.h>
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
    Instrumentation
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
    /// NO_SANITIZE_THREAD: this function is called from signal handlers where TSan cannot
    /// properly reason about concurrency. The pipe write uses DiscardOnFailure, so a
    /// write to a concurrently-closed fd is benign (results in EBADF, silently discarded).
    static NO_SANITIZE_THREAD void send(TraceType trace_type, const StackTrace & stack_trace, Extras extras);

private:
    friend class TraceCollector;
    static LazyPipeFDs pipe;
};

}
