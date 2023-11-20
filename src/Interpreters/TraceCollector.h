#pragma once
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
    explicit TraceCollector(std::shared_ptr<TraceLog> trace_log_);
    ~TraceCollector();

private:
    std::shared_ptr<TraceLog> trace_log;
    ThreadFromGlobalPool thread;

    void tryClosePipe();

    void run();
};

}
