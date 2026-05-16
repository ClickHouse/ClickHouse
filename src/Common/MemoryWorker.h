#pragma once

#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/ThreadPool.h>
#include <Common/Jemalloc.h>
#include <Common/PageCache.h>
#include <IO/ReadBufferFromFile.h>

#include <atomic>
#include <filesystem>
#include <memory>

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
    uint64_t decay_adjustment_period_ms = 0;
    bool use_cgroup = true;
    double dynamic_hard_limit_ratio = 0.0;
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

    /// Update the dynamic hard-limit settings from a config reload.
    ///   * `ceiling` is the configured `max_server_memory_usage` (after applying the ratio);
    ///     the dynamic adjustment may never set the hard limit above this. A value <= 0
    ///     disables the cap (i.e. unlimited).
    ///   * `ratio` is the `max_server_memory_usage_to_ram_ratio`; setting it to 0 disables
    ///     the dynamic adjustment entirely.
    ///
    /// Until this is called for the first time, the dynamic adjustment is suppressed, so
    /// the worker cannot inflate the hard limit before the server has had a chance to load
    /// its memory settings. Re-calling it on config reload keeps both values in sync.
    void setDynamicHardLimitSettings(Int64 ceiling, double ratio);

    ~MemoryWorker();
private:
    uint64_t getMemoryUsage(bool log_error);

    void updateResidentMemoryThread();

    ThreadFromGlobalPool update_resident_memory_thread;

    std::mutex rss_update_mutex;
    std::condition_variable rss_update_cv;

    std::mutex purge_dirty_pages_mutex;
    std::condition_variable purge_dirty_pages_cv;

    bool shutdown = false;

    LoggerPtr log;

    uint64_t rss_update_period_ms;

    bool correct_tracker = false;

    double purge_total_memory_threshold_ratio;
    double purge_dirty_pages_threshold_ratio;
    uint64_t page_size = 0;
    std::chrono::milliseconds decay_adjustment_period_ms{0};

    /// Scaling factor the dynamic adjustment applies to `resident + available` to compute
    /// the new hard limit. Mirrors `max_server_memory_usage_to_ram_ratio` and is refreshed
    /// on each config reload via `setDynamicHardLimitSettings`. A value <= 0 disables the
    /// dynamic adjustment.
    std::atomic<double> dynamic_hard_limit_ratio{0.0};

    /// Ceiling for the dynamic hard limit, set by Server.cpp / LocalServer.cpp.
    /// Initial value -1 means "not configured yet"; the dynamic adjustment then skips this tick.
    std::atomic<Int64> external_hard_limit{-1};

    /// Amount of memory that may still be allocated by ClickHouse, used as input to the
    /// dynamic hard-limit formula. Prefers the cgroup view (`memory.max` minus the cgroup's
    /// `memory.current`-equivalent via `cgroups_reader`) when running in a cgroup with a
    /// finite limit; otherwise falls back to `/proc/meminfo`'s `MemAvailable`.
    /// Returns 0 if no source is available.
    uint64_t readAvailableForDynamicLimit();

    /// Reads `MemAvailable` from /proc/meminfo. Returns 0 if the file can't be read.
    /// The lazily-opened buffer is owned by `updateResidentMemoryThread`, which is the only caller.
    uint64_t readSystemAvailableMemory();
    std::unique_ptr<ReadBufferFromFile> meminfo_buf;
    [[maybe_unused]] bool meminfo_warnings_printed = false;

    /// Open files for the effective cgroup memory limits. On cgroup v2, every ancestor
    /// cgroup up to the mount root has its own `memory.max`, and any of them can apply.
    /// We open all of them at startup and on each tick take the minimum finite value,
    /// so an ancestor's `memory.max = 10G` is respected even when the leaf cgroup has
    /// `memory.max = max`. On cgroup v1 there is a single `memory.limit_in_bytes` for
    /// the leaf cgroup, which is the only entry in the vector.
    std::vector<std::unique_ptr<ReadBufferFromFile>> cgroup_memory_max_bufs;
    [[maybe_unused]] bool cgroup_memory_max_warnings_printed = false;

    /// Bumped by `setDynamicHardLimitSettings` after writing the new ratio/ceiling.
    /// The worker captures the generation when it starts a dynamic update tick, then
    /// re-checks before calling `setHardLimit`. If the generation changed while the
    /// tick was in flight, a config reload took effect concurrently and the tick's
    /// computed value would be stale, so we skip applying it.
    std::atomic<uint64_t> settings_generation{0};

    MemoryUsageSource source{MemoryUsageSource::None};

    std::shared_ptr<ICgroupsReader> cgroups_reader;

    std::shared_ptr<PageCache> page_cache;

#if USE_JEMALLOC
    void purgeDirtyPagesThread();
    void setDirtyDecayForAllArenas(size_t decay_ms);

    ThreadFromGlobalPool purge_dirty_pages_thread;

    /// State machine for dynamic dirty pages decay control
    enum class DecayState : uint8_t
    {
        Enabled,           /// Decay is enabled (normal operation)
        DisableRequested,  /// Update thread requests disabling decay due to sustained memory pressure
        Disabled,          /// Decay is disabled (aggressive memory reclaim mode)
        EnableRequested    /// Update thread requests enabling decay after sustained normal conditions
    };

    /// Used to notify the purging thread about actions to take
    std::atomic<bool> purge_dirty_pages = false;

    /// Current state of the decay control state machine
    std::atomic<DecayState> decay_state{DecayState::Enabled};

    Jemalloc::MibCache<uint64_t> epoch_mib{"epoch"};
    Jemalloc::MibCache<size_t> resident_mib{"stats.resident"};
    Jemalloc::MibCache<size_t> pagesize_mib{"arenas.page"};
    Jemalloc::MibCache<size_t> dirty_decay_ms_mib{"arenas.dirty_decay_ms"};

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
    Jemalloc::MibCache<size_t> pdirty_mib{"stats.arenas." STRINGIFY(MALLCTL_ARENAS_ALL) ".pdirty"};
    Jemalloc::MibCache<size_t> purge_mib{"arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge"};
#undef STRINGIFY
#undef STRINGIFY_HELPER
#endif
};

}
