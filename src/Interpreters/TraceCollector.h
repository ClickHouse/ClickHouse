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

    ThreadFromGlobalPool thread;

    void tryClosePipe();

    void run();
};

}
