#pragma once

#include "Common/PipeFDs.h"
#include <Common/ThreadPool.h>

class StackTrace;

namespace Poco
{
    class Logger;
}

namespace DB
{

class TraceLog;

enum class TraceType : uint8_t
{
    Real,
    CPU,
    Memory,
    MemorySample
};

class TraceCollector
{
public:
    TraceCollector(std::shared_ptr<TraceLog> trace_log_);
    ~TraceCollector();

    /// Collect a stack trace. This method is signal safe.
    /// Precondition: the TraceCollector object must be created.
    /// size - for memory tracing is the amount of memory allocated; for other trace types it is 0.
    static void collect(TraceType trace_type, const StackTrace & stack_trace, Int64 size);

private:
    std::shared_ptr<TraceLog> trace_log;
    ThreadFromGlobalPool thread;

    void run();
    void stop();
};

}
