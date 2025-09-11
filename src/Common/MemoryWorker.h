#pragma once

#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/ThreadPool.h>
#include <Common/Jemalloc.h>
#include <Common/PageCache.h>

#include <filesystem>

namespace DB
{

struct ICgroupsReader
{
    enum class CgroupsVersion : uint8_t
    {
        V1,
        V2
    };

#if defined(OS_LINUX)
    static std::shared_ptr<ICgroupsReader>
    createCgroupsReader(ICgroupsReader::CgroupsVersion version, const std::filesystem::path & cgroup_path);
#endif

    virtual ~ICgroupsReader() = default;

    virtual uint64_t readMemoryUsage() = 0;

    virtual std::string dumpAllStats() = 0;
};


/// Correct MemoryTracker based on external information (e.g. Cgroups or stats.resident from jemalloc)
/// The worker spawns a background thread which periodically reads current resident memory from the source,
/// whose value is sent to global MemoryTracker.
/// It can do additional things like purging jemalloc dirty pages if the current memory usage is higher than global hard limit.
class MemoryWorker
{
public:
    MemoryWorker(uint64_t period_ms_, bool correct_tracker_, bool use_cgroup, std::shared_ptr<PageCache> page_cache_);

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
    bool correct_tracker = false;

    MemoryUsageSource source{MemoryUsageSource::None};

    std::shared_ptr<ICgroupsReader> cgroups_reader;

    std::shared_ptr<PageCache> page_cache;

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
