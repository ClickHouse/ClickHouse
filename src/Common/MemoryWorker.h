#pragma once

#include <Common/ThreadPool.h>

#include "config.h"

namespace DB
{

#if USE_JEMALLOC
/// Correct MemoryTracker based on stats.resident read from jemalloc.
/// This requires jemalloc built with --enable-stats which we use.
/// The worker spawns a background thread which moves the jemalloc epoch (updates internal stats),
/// and fetches the current stats.resident whose value is sent to global MemoryTracker.
/// Additionally, if the current memory usage is higher than global hard limit,
/// jemalloc's dirty pages are forcefully purged.
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
