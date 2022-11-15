#pragma once

#include <Common/ThreadPool.h>
#include <Common/TraceSender.h>

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
    explicit TraceCollector(std::shared_ptr<TraceLog> trace_log_);
    ~TraceCollector();

    static inline void collect(TraceType trace_type, const StackTrace & stack_trace, Int64 size)
    {
        return TraceSender::send(trace_type, stack_trace, size);
    }

private:
    std::shared_ptr<TraceLog> trace_log;
    ThreadFromGlobalPool thread;

    void run();
    void stop();
};

}
