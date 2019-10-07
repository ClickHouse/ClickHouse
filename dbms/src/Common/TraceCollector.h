#pragma once

#include <Common/ThreadPool.h>

namespace DB
{

class TraceLog;

class TraceCollector :  WithLogger<TraceCollector>
{
private:
    std::shared_ptr<TraceLog> trace_log;
    ThreadFromGlobalPool thread;

    void run();

    static void notifyToStop();

public:
    TraceCollector(std::shared_ptr<TraceLog> & trace_log_);

    ~TraceCollector();
};

}
