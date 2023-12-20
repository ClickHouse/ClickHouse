#pragma once
#include <Common/ThreadPool.h>
#include <Core/BackgroundSchedulePool.h>

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

    void setHeapProfilerDumpPeriod(Int64 seconds);
    void dumpHeapProfileNow();

private:
    std::shared_ptr<TraceLog> trace_log;
    ThreadFromGlobalPool thread;

    BackgroundSchedulePool::TaskHolder heap_profiler_task;
    std::atomic<Int64> heap_profiler_dump_period_seconds {-1};

    std::mutex manual_heap_dump_mutex;
    std::condition_variable manual_heap_dump_cv;
    std::atomic_bool manual_heap_dump_requested {false};

    void tryClosePipe();

    void run();

    void heapProfilerTask();
    void scheduleHeapProfilerTask();
};

}
