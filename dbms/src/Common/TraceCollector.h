#pragma once

#include <Common/ThreadPool.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

class TraceLog;

class TraceCollector
{
private:
    Poco::Logger * log;
    std::shared_ptr<TraceLog> trace_log;
    ThreadFromGlobalPool thread;

    void run();

    static void notifyToStop();

public:
    TraceCollector(std::shared_ptr<TraceLog> & trace_log_);

    ~TraceCollector();
};

}
