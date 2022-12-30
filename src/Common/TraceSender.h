#pragma once

#include <Common/PipeFDs.h>
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
};

/// This is the second part of TraceCollector, that sends stacktrace to the pipe.
/// It has been split out to avoid dependency from interpreters part.
class TraceSender
{
public:
    /// Collect a stack trace. This method is signal safe.
    /// Precondition: the TraceCollector object must be created.
    /// size - for memory tracing is the amount of memory allocated; for other trace types it is 0.
    static void send(TraceType trace_type, const StackTrace & stack_trace, Int64 size);

private:
    friend class TraceCollector;
    static LazyPipeFDs pipe;
};

}
