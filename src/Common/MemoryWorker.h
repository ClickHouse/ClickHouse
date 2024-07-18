#pragma once

#include <Common/ThreadPool.h>

#include "config.h"

namespace DB
{

#if USE_JEMALLOC
class MemoryWorker
{
public:
    explicit MemoryWorker(uint64_t period_ms_);

    ~MemoryWorker();
private:
    void backgroundThread();

    ThreadFromGlobalPool background_thread;

    std::mutex mutex;
    std::condition_variable cv;
    bool shutdown = false;

    std::chrono::milliseconds period_ms;
};
#else
class MemoryWorker
{
public:
    explicit MemoryWorker(uint64_t /*period_ms_*/) {}
};
#endif

}
