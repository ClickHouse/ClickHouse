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

enum class TraceType : UInt8
{
    REAL_TIME,
    CPU_TIME,
    MEMORY,
};

class TraceCollector
{
public:
    TraceCollector();
    ~TraceCollector();

    void setTraceLog(const std::shared_ptr<TraceLog> & trace_log_) { trace_log = trace_log_; }

    void collect(TraceType type, const StackTrace & stack_trace, int overrun_count = 0);
    void collect(UInt64 size);

private:
    std::shared_ptr<TraceLog> trace_log;
    ThreadFromGlobalPool thread;
    LazyPipeFDs pipe;

    void run();
    void stop();
};

}
