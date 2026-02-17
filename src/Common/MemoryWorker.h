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

    /// Return <path, version>
    static std::pair<std::string, CgroupsVersion> getCgroupsPath();
#endif

    virtual ~ICgroupsReader() = default;

    virtual uint64_t readMemoryUsage() = 0;

    virtual std::string dumpAllStats() = 0;
};

struct MemoryWorkerConfig
{
    uint64_t rss_update_period_ms = 0;
    double purge_dirty_pages_threshold_ratio = 0.0;
    double purge_total_memory_threshold_ratio = 0.0;
    bool correct_tracker = false;
    bool use_cgroup = true;
};

/// Correct MemoryTracker based on external information (e.g. Cgroups or stats.resident from jemalloc)
/// The worker spawns a background thread which periodically reads current resident memory from the source,
/// whose value is sent to global MemoryTracker.
/// It can do additional things like purging jemalloc dirty pages if the current memory usage is higher than global hard limit.
class MemoryWorker
{
public:
    MemoryWorker(MemoryWorkerConfig config, std::shared_ptr<PageCache> page_cache_);

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
    uint64_t getMemoryUsage(bool log_error);

    void updateResidentMemoryThread();
    [[maybe_unused]] void purgeDirtyPagesThread();

    ThreadFromGlobalPool update_resident_memory_thread;
    ThreadFromGlobalPool purge_dirty_pages_thread;

    std::mutex rss_update_mutex;
    std::condition_variable rss_update_cv;
    std::mutex purge_dirty_pages_mutex;
    std::condition_variable purge_dirty_pages_cv;
    bool shutdown = false;

    LoggerPtr log;

    uint64_t rss_update_period_ms;

    bool correct_tracker = false;

    std::atomic<bool> purge_dirty_pages = false;
    double purge_total_memory_threshold_ratio;
    double purge_dirty_pages_threshold_ratio;
    uint64_t page_size = 0;

    MemoryUsageSource source{MemoryUsageSource::None};

    std::shared_ptr<ICgroupsReader> cgroups_reader;

    std::shared_ptr<PageCache> page_cache;

#if USE_JEMALLOC
    Jemalloc::MibCache<uint64_t> epoch_mib{"epoch"};
    Jemalloc::MibCache<size_t> resident_mib{"stats.resident"};
    Jemalloc::MibCache<size_t> pagesize_mib{"arenas.page"};

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
    Jemalloc::MibCache<size_t> pdirty_mib{"stats.arenas." STRINGIFY(MALLCTL_ARENAS_ALL) ".pdirty"};
    Jemalloc::MibCache<size_t> purge_mib{"arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge"};
#undef STRINGIFY
#undef STRINGIFY_HELPER
#endif
};

}
