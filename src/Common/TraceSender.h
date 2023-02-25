#pragma once

#include <Common/PipeFDs.h>
#include <Common/ProfileEvents.h>
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
};

/// This is the second part of TraceCollector, that sends stacktrace to the pipe.
/// It has been split out to avoid dependency from interpreters part.
class TraceSender
{
public:
    struct Extras
    {
        /// size - for memory tracing is the amount of memory allocated; for other trace types it is 0.
        Int64 size{};
        /// Event type and increment for 'ProfileEvent' trace type; for other trace types defaults.
        ProfileEvents::Event event{ProfileEvents::end()};
        ProfileEvents::Count increment{};
    };

    /// Collect a stack trace. This method is signal safe.
    /// Precondition: the TraceCollector object must be created.
    static void send(TraceType trace_type, const StackTrace & stack_trace, Extras extras);

private:
    friend class TraceCollector;
    static LazyPipeFDs pipe;
};

}
