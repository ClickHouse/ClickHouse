#pragma once
#include <atomic>
#include <Common/ThreadPool.h>

class StackTrace;

namespace Poco
{
    class Logger;
}

namespace DB
{

class TraceLog;

class TraceCollector
{
public:
    TraceCollector();
    ~TraceCollector();

    void initialize(std::shared_ptr<TraceLog> trace_log_);

private:
    std::shared_ptr<TraceLog> getTraceLog();

    std::atomic<bool> is_trace_log_initialized = false;
    std::shared_ptr<TraceLog> trace_log_ptr;
    bool symbolize = false;

    /// Use a thread that does not call `ThreadStatus::initGlobalProfiler` on startup:
    /// `initGlobalProfiler` reads `Context::hasTraceCollector`, which races with
    /// `optional<TraceCollector>::emplace` because the worker thread is created
    /// inside the optional's constructor (before `emplace` has set `has_value`).
    /// Profiling the trace collector's own thread is also pointless.
    ThreadFromGlobalPoolWithoutTraceCollector thread;

    void tryClosePipe();
    void run();
};

}
