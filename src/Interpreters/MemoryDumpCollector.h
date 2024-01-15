#pragma once

#include <Common/ThreadPool.h>

namespace DB
{

class MemoryDumpLog;

class MemoryDumpCollector
{
public:
    explicit MemoryDumpCollector(std::shared_ptr<MemoryDumpLog> memory_dump_log_, UInt64 memory_dump_interval_ms_);

    ~MemoryDumpCollector();

private:
    std::shared_ptr<MemoryDumpLog> memory_dump_log;

    UInt64 memory_dump_interval_ms;

    ThreadFromGlobalPool thread;

    std::condition_variable cond;
    std::mutex cond_mutex;
    std::atomic<bool> stopped{false};

    void run();
};

}
