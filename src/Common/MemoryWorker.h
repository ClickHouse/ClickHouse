#pragma once

#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/ThreadPool.h>
#include <Common/Jemalloc.h>

namespace DB
{

struct ICgroupsReader;

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

    enum class MemoryUsageSource : uint8_t
    {
        None,
        Cgroups,
        Jemalloc
    };

    MemoryUsageSource getSource();

    void start();

    ~MemoryWorker();
private:
    uint64_t getMemoryUsage();

    void backgroundThread();

    ThreadFromGlobalPool background_thread;

    std::mutex mutex;
    std::condition_variable cv;
    bool shutdown = false;

    LoggerPtr log;

    uint64_t period_ms;

    MemoryUsageSource source{MemoryUsageSource::None};

#if defined(OS_LINUX)
    std::shared_ptr<ICgroupsReader> cgroups_reader;
#endif

#if USE_JEMALLOC
    JemallocMibCache<uint64_t> epoch_mib{"epoch"};
    JemallocMibCache<size_t> resident_mib{"stats.resident"};
    JemallocMibCache<size_t> allocated_mib{"stats.allocated"};

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
    JemallocMibCache<size_t> purge_mib{"arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge"};
#undef STRINGIFY
#undef STRINGIFY_HELPER
#endif
};

}
