#include <Common/MemoryWorker.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <base/cgroupsv2.h>
#include <base/getMemoryAmount.h>
#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#include <Common/OSThreadNiceValue.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <fmt/ranges.h>

#include <filesystem>
#include <optional>

#include <unistd.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
    extern const Event MemoryWorkerRun;
    extern const Event MemoryWorkerRunElapsedMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

#if defined(OS_LINUX)
namespace
{

/// Format is
///   kernel 5
///   rss 15
///   [...]
std::map<std::string, uint64_t> readAllMetricsFromStatFile(ReadBufferFromFile & buf)
{
    std::map<std::string, uint64_t> metrics;
    while (!buf.eof())
    {
        std::string current_key;
        readStringUntilWhitespace(current_key, buf);

        assertChar(' ', buf);

        uint64_t value = 0;
        readIntText(value, buf);
        assertChar('\n', buf);

        auto [_, inserted] = metrics.emplace(std::move(current_key), value);
        chassert(inserted, "Duplicate keys in stat file");
    }
    return metrics;
}

using Metrics = std::map<std::string_view, uint64_t>;

void readMetricsFromStatFile(
    ReadBufferFromFile & buf,
    Metrics & metrics,
    std::initializer_list<std::string_view> keys,
    bool * warnings_printed)
{
    /// Zero out existing values; keeps map nodes allocated for reuse.
    for (auto & [_, v] : metrics)
        v = 0;

    /// Track which keys were actually seen in this pass.
    uint64_t seen_mask = 0;

    bool print_warnings = !*warnings_printed;
    while (!buf.eof())
    {
        std::string current_key;
        readStringUntilWhitespace(current_key, buf);

        const auto * it = std::find(keys.begin(), keys.end(), current_key);
        if (it == keys.end())
        {
            std::string dummy;
            readStringUntilNewlineInto(dummy, buf);
            buf.tryIgnore(1);
            continue;
        }

        assertChar(' ', buf);
        uint64_t value = 0;
        readIntText(value, buf);
        buf.tryIgnore(1);

        uint64_t key_bit = 1ull << (it - keys.begin());
        if (seen_mask & key_bit)
        {
            if (print_warnings)
            {
                *warnings_printed = true;
                LOG_ERROR(getLogger("CgroupsReader"), "Duplicate key '{}' in '{}'", current_key, buf.getFileName());
            }
        }
        seen_mask |= key_bit;

        /// Use the string_view from keys (string literals) as map key.
        metrics[*it] = value;
    }

    if (print_warnings)
    {
        for (const auto * it = keys.begin(); it != keys.end(); ++it)
        {
            uint64_t key_bit = 1ull << (it - keys.begin());
            if (!(seen_mask & key_bit))
            {
                *warnings_printed = true;
                LOG_ERROR(getLogger("CgroupsReader"), "Cannot find '{}' in '{}'", *it, buf.getFileName());
            }
        }
    }
}

struct CgroupsV1Reader : ICgroupsReader
{
    explicit CgroupsV1Reader(const fs::path & stat_file_dir) : buf(stat_file_dir / "memory.stat") { }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        buf.rewind();
        readMetricsFromStatFile(buf, metrics, {"rss"}, &warnings_printed);
        auto it = metrics.find("rss");
        return it != metrics.end() ? it->second : 0;
    }

    std::string dumpAllStats() override
    {
        std::lock_guard lock(mutex);
        buf.rewind();
        return fmt::format("{}", readAllMetricsFromStatFile(buf));
    }

private:
    std::mutex mutex;
    ReadBufferFromFile buf TSA_GUARDED_BY(mutex);
    Metrics metrics TSA_GUARDED_BY(mutex);
    bool warnings_printed TSA_GUARDED_BY(mutex) = false;
};

struct CgroupsV2Reader : ICgroupsReader
{
    explicit CgroupsV2Reader(const fs::path & stat_file_dir) : stat_buf(stat_file_dir / "memory.stat") { }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        stat_buf.rewind();
        readMetricsFromStatFile(
            stat_buf, metrics, {"anon", "sock", "kernel", "slab_reclaimable"}, &warnings_printed);

        auto get = [](const Metrics & m, std::string_view key) -> uint64_t
        {
            auto it = m.find(key);
            return it != m.end() ? it->second : 0;
        };

        /// anon + sock: actual process memory.
        /// kernel - slab_reclaimable: non-reclaimable kernel memory (pagetables, kernel_stack, slab_unreclaimable).
        /// slab_reclaimable is excluded because the kernel reclaims it synchronously under memory pressure
        /// before invoking the OOM killer, so it should not count against the application's memory budget.
        uint64_t usage = get(metrics, "anon") + get(metrics, "sock");
        uint64_t kernel = get(metrics, "kernel");
        uint64_t slab_reclaimable = get(metrics, "slab_reclaimable");
        if (kernel > slab_reclaimable)
            usage += kernel - slab_reclaimable;
        return usage;
    }

    std::string dumpAllStats() override
    {
        std::lock_guard lock(mutex);
        stat_buf.rewind();
        return fmt::format("{}", readAllMetricsFromStatFile(stat_buf));
    }

private:
    std::mutex mutex;
    ReadBufferFromFile stat_buf TSA_GUARDED_BY(mutex);
    Metrics metrics TSA_GUARDED_BY(mutex);
    bool warnings_printed TSA_GUARDED_BY(mutex) = false;
};

/// Caveats:
/// - All of the logic in this file assumes that the current process is the only process in the
///   containing cgroup (or more precisely: the only process with significant memory consumption).
///   If this is not the case, then other processe's memory consumption may affect the internal
///   memory tracker ...
/// - Cgroups v1 and v2 allow nested cgroup hierarchies. As v1 is deprecated for over half a
///   decade and will go away at some point, hierarchical detection is only implemented for v2.
/// - I did not test what happens if a host has v1 and v2 simultaneously enabled. I believe such
///   systems existed only for a short transition period.

std::optional<std::string> getCgroupsV1Path()
{
    auto path = default_cgroups_mount / "memory/memory.stat";
    if (!fs::exists(path))
        return {};
    return {default_cgroups_mount / "memory"};
}

}

std::pair<std::string, ICgroupsReader::CgroupsVersion> ICgroupsReader::getCgroupsPath()
{
    auto v2_path = getCgroupsV2PathContainingFile("memory.current");
    if (v2_path.has_value())
        return {*v2_path, ICgroupsReader::CgroupsVersion::V2};

    auto v1_path = getCgroupsV1Path();
    if (v1_path.has_value())
        return {*v1_path, ICgroupsReader::CgroupsVersion::V1};

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot find cgroups v1 or v2 current memory file");
}

std::shared_ptr<ICgroupsReader> ICgroupsReader::createCgroupsReader(ICgroupsReader::CgroupsVersion version, const std::filesystem::path & cgroup_path)
{
    if (version == CgroupsVersion::V2)
        return std::make_shared<CgroupsV2Reader>(cgroup_path);

    chassert(version == CgroupsVersion::V1);
    return std::make_shared<CgroupsV1Reader>(cgroup_path);
}
#endif

namespace
{

std::string_view sourceToString(MemoryWorker::MemoryUsageSource source)
{
    switch (source)
    {
        case MemoryWorker::MemoryUsageSource::Cgroups: return "Cgroups";
        case MemoryWorker::MemoryUsageSource::Jemalloc: return "Jemalloc";
        case MemoryWorker::MemoryUsageSource::None: return "None";
    }
}

}

/// We try to pick the best possible supported source for reading memory usage.
/// Supported sources in order of priority
/// - reading from cgroups' pseudo-files (fastest and most accurate)
/// - reading jemalloc's resident stat (doesn't take into account allocations that didn't use jemalloc)
/// Also, different tick rates are used because not all options are equally fast
MemoryWorker::MemoryWorker(
    MemoryWorkerConfig config,
    std::shared_ptr<PageCache> page_cache_)
    : log(getLogger("MemoryWorker"))
    , rss_update_period_ms(config.rss_update_period_ms)
    , correct_tracker(config.correct_tracker)
    , purge_total_memory_threshold_ratio(config.purge_total_memory_threshold_ratio)
    , purge_dirty_pages_threshold_ratio(config.purge_dirty_pages_threshold_ratio)
    , decay_adjustment_period_ms(config.decay_adjustment_period_ms)
    , dynamic_hard_limit_ratio(config.dynamic_hard_limit_ratio)
    , page_cache(page_cache_)
{
#if USE_JEMALLOC
    page_size = pagesize_mib.getValue();
#endif

    /// Captured once for use in `readAvailableForDynamicLimit` to detect the
    /// cgroup v1 "no limit" sentinel. We deliberately use `getMemoryAmountOrZero`
    /// rather than the cgroup-aware `getMemoryAmount` so that nested cgroups
    /// with their own finite limits do not also poison this threshold; here we
    /// want only the host's physical RAM.
    {
        int64_t num_pages = sysconf(_SC_PHYS_PAGES);
        int64_t page_size_bytes = sysconf(_SC_PAGESIZE);
        if (num_pages > 0 && page_size_bytes > 0)
            host_memory_bytes = static_cast<uint64_t>(num_pages) * static_cast<uint64_t>(page_size_bytes);
    }

    if (config.use_cgroup)
    {
#if defined(OS_LINUX)
        try
        {
            static constexpr uint64_t cgroups_memory_usage_tick_ms{50};

            const auto [cgroup_path, version] = ICgroupsReader::getCgroupsPath();
            LOG_INFO(
                getLogger("CgroupsReader"),
                "Will create cgroup reader from '{}' (cgroups version: {})",
                cgroup_path,
                (version == ICgroupsReader::CgroupsVersion::V1) ? "v1" : "v2");

            cgroups_reader = ICgroupsReader::createCgroupsReader(version, cgroup_path);
            source = MemoryUsageSource::Cgroups;
            if (rss_update_period_ms == 0)
                rss_update_period_ms = cgroups_memory_usage_tick_ms;

            /// Open files for the cgroup memory limit so the dynamic hard-limit
            /// adjustment can read them cheaply on each tick. v1 and v2 use different
            /// file names and v2 uses a hierarchy.
            if (version == ICgroupsReader::CgroupsVersion::V2)
            {
                /// In cgroup v2, every ancestor cgroup has its own `memory.max` and
                /// `memory.current`. We pair them at the same level so the computed
                /// per-level `available_i = max_i - current_i` reflects sibling
                /// consumption inside an ancestor: using the leaf's `memory.current`
                /// against an ancestor's `memory.max` would ignore other children of
                /// that ancestor and let us exceed its budget.
                fs::path current = fs::path(cgroup_path);
                while (current != default_cgroups_mount.parent_path())
                {
                    fs::path max_path = current / "memory.max";
                    fs::path current_path = current / "memory.current";
                    if (fs::exists(max_path) && fs::exists(current_path))
                    {
                        try
                        {
                            CgroupMemoryLevel level;
                            level.max_buf = std::make_unique<ReadBufferFromFile>(max_path.string());
                            level.current_buf = std::make_unique<ReadBufferFromFile>(current_path.string());
                            cgroup_memory_levels.push_back(std::move(level));
                        }
                        catch (...)
                        {
                            tryLogCurrentException(log, fmt::format("Cannot open cgroup memory files at '{}'", current.string()));
                        }
                    }
                    current = current.parent_path();
                }
            }
            else
            {
                fs::path memory_max_path = fs::path(cgroup_path) / "memory.limit_in_bytes";
                if (fs::exists(memory_max_path))
                {
                    try
                    {
                        CgroupMemoryLevel level;
                        level.max_buf = std::make_unique<ReadBufferFromFile>(memory_max_path.string());
                        /// v1 has no per-level `memory.current` analogue we use here;
                        /// leaf usage comes from `cgroups_reader` in `readAvailableForDynamicLimit`.
                        cgroup_memory_levels.push_back(std::move(level));
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, "Cannot open cgroup memory limit file");
                    }
                }
            }

            return;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot use cgroups reader");
        }
#endif
    }

#if USE_JEMALLOC
    static constexpr uint64_t jemalloc_memory_usage_tick_ms{100};

    source = MemoryUsageSource::Jemalloc;
    if (rss_update_period_ms == 0)
        rss_update_period_ms = jemalloc_memory_usage_tick_ms;
#endif
}

MemoryWorker::MemoryUsageSource MemoryWorker::getSource()
{
    return source;
}

void MemoryWorker::start()
{
    if (source == MemoryUsageSource::None)
        return;

    const std::string purge_dirty_pages_info = purge_dirty_pages_threshold_ratio > 0 || purge_total_memory_threshold_ratio > 0
        ? fmt::format(
              "enabled (total memory threshold ratio: {}, dirty pages threshold ratio: {}, page size: {})",
              purge_total_memory_threshold_ratio,
              purge_dirty_pages_threshold_ratio,
              page_size)
        : "disabled";

    LOG_INFO(
        log,
        "Starting background memory thread with period of {}ms, using {} as source, purging dirty pages {}",
        rss_update_period_ms,
        sourceToString(source),
        purge_dirty_pages_info);

    update_resident_memory_thread = ThreadFromGlobalPool([this] { updateResidentMemoryThread(); });

#if USE_JEMALLOC
    purge_dirty_pages_thread = ThreadFromGlobalPool([this] { purgeDirtyPagesThread(); });
#endif
}

MemoryWorker::~MemoryWorker()
{
    {
        std::scoped_lock lock(rss_update_mutex, purge_dirty_pages_mutex);
        shutdown = true;
    }

    rss_update_cv.notify_all();
    purge_dirty_pages_cv.notify_all();

    if (update_resident_memory_thread.joinable())
        update_resident_memory_thread.join();

#if USE_JEMALLOC
    if (purge_dirty_pages_thread.joinable())
        purge_dirty_pages_thread.join();
#endif
}

uint64_t MemoryWorker::getMemoryUsage(bool log_error)
{
    switch (source)
    {
        case MemoryUsageSource::Cgroups:
        {
            if (cgroups_reader != nullptr)
                return cgroups_reader->readMemoryUsage();
            [[fallthrough]];
        }
        case MemoryUsageSource::Jemalloc:
#if USE_JEMALLOC
            epoch_mib.setValue(0);
            return resident_mib.getValue();
#else
            [[fallthrough]];
#endif
        case MemoryUsageSource::None:
        {
            if (log_error)
                LOG_ERROR(log, "Trying to fetch memory usage while no memory source can be used");
            return 0;
        }
    }
}

namespace
{

[[maybe_unused]] std::chrono::milliseconds getCurrentTimeMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch());
}

}

void MemoryWorker::setDynamicHardLimitSettings(Int64 ceiling, double ratio)
{
    /// Hold the mutex around the whole sequence of "store new settings" and
    /// "install new hard limit". The worker re-checks `settings_generation`
    /// under the same mutex before calling `setHardLimit`, so an in-flight
    /// tick that computed against the old ratio cannot win the race against
    /// this reload — it observes either the old generation while we still
    /// hold the mutex (and waits), or the bumped generation (and skips).
    std::lock_guard lock(dynamic_hard_limit_apply_mutex);

    /// Order matters: write the ratio first. The worker thread reads `external_hard_limit`
    /// first, and only proceeds with the adjustment when it is >= 0. By the time the
    /// adjustment is enabled (ceiling becomes >= 0), the new ratio is already visible.
    dynamic_hard_limit_ratio.store(ratio, std::memory_order_relaxed);
    external_hard_limit.store(ceiling, std::memory_order_relaxed);
    /// Bump generation *after* writing values so the worker, which reads the
    /// generation before and after its tick, can detect a reload that happened
    /// in flight and skip applying a stale `setHardLimit`. Release pairs with the
    /// worker's acquire load when re-checking.
    settings_generation.fetch_add(1, std::memory_order_release);

    /// Install the configured ceiling as the current hard limit while we still
    /// hold the mutex. Doing this here (instead of in the caller, before
    /// `setDynamicHardLimitSettings`) closes the race window where the worker
    /// could overwrite an out-of-band `setHardLimit` with a stale value before
    /// it observed the new generation.
    total_memory_tracker.setHardLimit(ceiling);
}

std::optional<uint64_t> MemoryWorker::readAvailableForDynamicLimit()
{
#if defined(OS_LINUX)
    /// When running in a cgroup with a finite limit, the host-wide `/proc/meminfo` is
    /// the wrong source: the cgroup may be much smaller than the host, and other
    /// processes in the cgroup count toward our budget. Use the cgroup view instead,
    /// the same way `AsynchronousMetrics` reports `CGroupMemoryTotal` / `CGroupMemoryUsed`.
    ///
    /// In cgroup v2, walk all ancestors and compute per-level headroom
    /// `available_i = memory.max_i - memory.current_i`, then take the minimum. Pairing
    /// max and current at the *same* level matters: an ancestor's `memory.current`
    /// includes sibling cgroups under that ancestor, while the leaf's `memory.current`
    /// does not. Using the leaf's usage against an ancestor's limit would ignore
    /// siblings and let the dynamic hard limit exceed the ancestor's remaining budget,
    /// which can still trigger a cgroup OOM kill.
    /// If *no* level has a finite limit, the cgroup has no memory limit at all, and
    /// `/proc/meminfo` (host-wide) is the right source.
    if (cgroups_reader && !cgroup_memory_levels.empty())
    {
        uint64_t min_available = std::numeric_limits<uint64_t>::max();
        bool any_finite = false;
        bool any_read_failure = false;
        for (auto & level : cgroup_memory_levels)
        {
            try
            {
                level.max_buf->rewind();
                /// `memory.max` value `"max"` means "no limit at this level". Handle it
                /// explicitly so the common path doesn't depend on parse-failure semantics.
                String first_token;
                readStringUntilWhitespace(first_token, *level.max_buf);
                if (first_token == "max")
                    continue;

                uint64_t limit_bytes = 0;
                ReadBufferFromString token_buf(first_token);
                if (!tryReadIntText(limit_bytes, token_buf) || limit_bytes == 0)
                    continue;

                /// On cgroup v1, `memory.limit_in_bytes` uses a huge sentinel value
                /// (`PAGE_COUNTER_MAX`, around `2^63`) to mean "no limit". On a host
                /// without cgroup memory limits this looks like a finite limit far
                /// above any real RAM amount and would otherwise pin the dynamic
                /// limit to the startup ceiling. Anything `>= host_memory_bytes` is
                /// effectively unbounded, so treat it the same as the v2 `"max"` token.
                if (host_memory_bytes != 0 && limit_bytes >= host_memory_bytes)
                    continue;

                uint64_t used = 0;
                if (level.current_buf)
                {
                    /// v2: read `memory.current` for the same level, so sibling
                    /// consumption inside an ancestor counts against that ancestor's budget.
                    level.current_buf->rewind();
                    readIntText(used, *level.current_buf);
                }
                else
                {
                    /// v1: the only opened level is the leaf cgroup; use the same usage
                    /// source as `cgroups_reader`. v1 does not traverse the hierarchy.
                    used = cgroups_reader->readMemoryUsage();
                }

                uint64_t available = (limit_bytes > used) ? (limit_bytes - used) : 0;
                min_available = std::min(min_available, available);
                any_finite = true;
            }
            catch (...)
            {
                any_read_failure = true;
                if (!std::exchange(cgroup_memory_max_warnings_printed, true))
                    tryLogCurrentException(log, "Cannot read cgroup memory limit/current");
            }
        }
        /// Fail-close on any per-level read failure. If even one ancestor's
        /// `memory.max`/`memory.current` could not be read, the omitted level may
        /// have been more restrictive than the levels we observed, so `min_available`
        /// over only the successful subset can overestimate the real headroom.
        /// Falling through to host-wide `/proc/meminfo` would compound the problem
        /// on containerized deployments, where the host's free memory can be far
        /// above the cgroup budget, and using it as the headroom estimate can let
        /// `total_memory_tracker` grow past the cgroup limit and trigger a cgroup
        /// OOM kill. Skip the adjustment this tick; the worker will retry next tick.
        if (any_read_failure)
            return std::nullopt;
        if (any_finite)
        {
            /// `min_available == 0` is a real "at or over the binding limit" signal,
            /// not a read failure. The caller's `used + safety_margin` clamp will keep
            /// the dynamic limit from strangling in-flight queries, while still
            /// shrinking the budget at the highest-pressure point.
            return min_available;
        }
    }
#endif
    return readSystemAvailableMemory();
}

std::optional<uint64_t> MemoryWorker::readSystemAvailableMemory()
{
#if defined(OS_LINUX)
    static constexpr std::string_view path = "/proc/meminfo";

    try
    {
        if (!meminfo_buf)
            meminfo_buf = std::make_unique<ReadBufferFromFile>(std::string{path});
        meminfo_buf->rewind();

        while (!meminfo_buf->eof())
        {
            std::string name;
            readStringUntilWhitespace(name, *meminfo_buf);
            skipWhitespaceIfAny(*meminfo_buf, true);

            uint64_t kb = 0;
            readIntText(kb, *meminfo_buf);

            if (name == "MemAvailable:")
                return kb * 1024ULL;

            skipToNextLineOrEOF(*meminfo_buf);
        }

        if (!std::exchange(meminfo_warnings_printed, true))
            LOG_ERROR(log, "Cannot find 'MemAvailable' in '{}'", path);
        return std::nullopt;
    }
    catch (...)
    {
        if (!std::exchange(meminfo_warnings_printed, true))
            tryLogCurrentException(log, fmt::format("Cannot read '{}'", path));
        /// Reopen on next attempt in case the descriptor became unusable.
        meminfo_buf.reset();
        return std::nullopt;
    }
#else
    return std::nullopt;
#endif
}

void MemoryWorker::updateResidentMemoryThread()
{
    DB::setThreadName(ThreadName::MEMORY_WORKER);

    /// Set the biggest priority for this thread to avoid drift
    /// under the CPU starvation.
    OSThreadNiceValue::set(-20);

    std::chrono::milliseconds chrono_period_ms{rss_update_period_ms};
    [[maybe_unused]] bool first_run = true;
    std::unique_lock rss_update_lock(rss_update_mutex);

#if USE_JEMALLOC
    /// First time we switched the state of purging dirty pages (purging -> not purging OR not purging -> purging)
    bool purging_dirty_pages = false;
    std::chrono::milliseconds purge_state_change_time_ms{0};
#endif

    while (true)
    {
        try
        {
            rss_update_cv.wait_for(rss_update_lock, chrono_period_ms, [this] { return shutdown; });
            if (shutdown)
                return;

            Stopwatch total_watch;

            Int64 resident = getMemoryUsage(first_run);
            MemoryTracker::updateRSS(resident);

            if (page_cache)
                page_cache->autoResize(std::max(resident, total_memory_tracker.get()), total_memory_tracker.getHardLimit());

#if USE_JEMALLOC
            const auto memory_tracker_limit = total_memory_tracker.getHardLimit();
            const auto purge_total_memory_threshold = static_cast<double>(memory_tracker_limit) * purge_total_memory_threshold_ratio;
            const auto purge_dirty_pages_threshold = static_cast<double>(memory_tracker_limit) * purge_dirty_pages_threshold_ratio;

            const bool needs_purge
                = (purge_total_memory_threshold_ratio > 0 && static_cast<double>(resident) > purge_total_memory_threshold)
                || (purge_dirty_pages_threshold_ratio > 0
                    && static_cast<double>(pdirty_mib.getValue() * page_size) > purge_dirty_pages_threshold);

            auto current_decay_state = decay_state.load(std::memory_order_relaxed);
            if (needs_purge)
            {
                bool notify_purge = false;
                if (decay_adjustment_period_ms.count() > 0)
                {
                    if (!std::exchange(purging_dirty_pages, true))
                    {
                        /// Transitioned into purging state, record the time
                        purge_state_change_time_ms = getCurrentTimeMs();
                    }
                    else if (
                        (getCurrentTimeMs() - purge_state_change_time_ms >= decay_adjustment_period_ms)
                        && current_decay_state == MemoryWorker::DecayState::Enabled)
                    {
                        /// Sustained memory pressure - request disabling decay
                        MemoryWorker::DecayState expected = MemoryWorker::DecayState::Enabled;
                        notify_purge |= decay_state.compare_exchange_strong(
                            expected, MemoryWorker::DecayState::DisableRequested, std::memory_order_relaxed);
                    }
                }

                if (current_decay_state != MemoryWorker::DecayState::Disabled)
                {
                    /// Trigger immediate purge if decay is not yet disabled
                    bool expected_purge_dirty_pages = false;
                    notify_purge |= purge_dirty_pages.compare_exchange_strong(expected_purge_dirty_pages, true, std::memory_order_relaxed);
                }

                if (notify_purge)
                    purge_dirty_pages_cv.notify_all();
            }
            else if (decay_adjustment_period_ms.count() > 0)
            {
                if (std::exchange(purging_dirty_pages, false))
                {
                    /// Transitioned out of purging state, record the time
                    purge_state_change_time_ms = getCurrentTimeMs();
                }
                else if (
                    (getCurrentTimeMs() - purge_state_change_time_ms >= decay_adjustment_period_ms)
                    && current_decay_state == MemoryWorker::DecayState::Disabled)
                {
                    /// Sustained normal conditions - request enabling decay
                    MemoryWorker::DecayState expected = MemoryWorker::DecayState::Disabled;
                    if (decay_state.compare_exchange_strong(expected, MemoryWorker::DecayState::EnableRequested, std::memory_order_relaxed))
                    {
                        purge_dirty_pages_cv.notify_all();
                    }
                }
            }

            /// update MemoryTracker with `allocated` information from jemalloc when:
            ///  - it's a first run of MemoryWorker (MemoryTracker could've missed some allocation before its initialization)
            ///  - MemoryTracker stores a negative value
            ///  - `correct_tracker` is set to true
            if (first_run || total_memory_tracker.get() < 0) [[unlikely]]
                MemoryTracker::updateAllocated(resident, /*log_change=*/true);
            else if (correct_tracker)
                MemoryTracker::updateAllocated(resident, /*log_change=*/false);
#else
            /// we don't update in the first run if we don't have jemalloc
            /// because we can only use resident memory information
            /// resident memory can be much larger than the actual allocated memory
            /// so we rather ignore the potential difference caused by allocated memory
            /// before MemoryTracker initialization
            if (total_memory_tracker.get() < 0 || correct_tracker) [[unlikely]]
                MemoryTracker::updateAllocated(resident, /*log_change=*/false);
#endif

            /// Capture the settings generation before reading ratio/ceiling. We re-read
            /// it just before `setHardLimit` and skip the write if a reload happened
            /// concurrently — otherwise the worker could overwrite the value the reload
            /// just installed with one computed from the old ratio.
            const uint64_t gen_before = settings_generation.load(std::memory_order_acquire);
            const double ratio = dynamic_hard_limit_ratio.load(std::memory_order_relaxed);
            if (ratio > 0.0)
            {
                /// Suppress the adjustment until the server has had a chance to call
                /// `setDynamicHardLimitSettings`. Otherwise we'd inflate the hard limit during
                /// the brief window between `MemoryWorker::start` and the first config
                /// reload that computes `max_server_memory_usage`.
                Int64 ceiling = external_hard_limit.load(std::memory_order_relaxed);
                if (ceiling >= 0)
                {
                    /// Distinguish "couldn't read the metric" (`nullopt`, skip this tick) from
                    /// "metric is genuinely zero" (`0`, real high-pressure signal). Skipping
                    /// on `0` would keep the previous (larger) hard limit in place exactly
                    /// when ClickHouse should be shrinking its budget the most.
                    std::optional<uint64_t> available_opt = readAvailableForDynamicLimit();
                    if (available_opt)
                    {
                        uint64_t available = *available_opt;
                        /// Use `resident` (jemalloc RSS or cgroup `memory.current`) as the baseline
                        /// of "memory we already own", not the MemoryTracker counter. The tracker
                        /// only counts allocations it sees through `Allocator`; jemalloc-internal
                        /// fragmentation, mmap'd pages, page cache, and any untracked allocation
                        /// are excluded. Under load `tracked` can be orders of magnitude smaller
                        /// than the actual RSS, which makes `(tracked + available) * ratio` compute
                        /// a hard limit close to current RSS and reject every subsequent allocation.
                        Int64 used = std::max<Int64>(0, resident);
                        /// `used + available` is the upper bound of memory we could potentially own:
                        /// what we already use plus what is still free in our cgroup (or on the host).
                        /// Scaling by `ratio < 1` leaves headroom for other processes on the host.
                        auto new_hard_limit = static_cast<Int64>(
                            static_cast<double>(static_cast<uint64_t>(used) + available) * ratio);

                        /// Under high memory pressure the formula can produce `new_hard_limit <= used`.
                        /// Setting the hard limit at or below current RSS would reject every new
                        /// allocation and break in-flight queries — the server cannot release memory
                        /// instantly. But skipping the tick entirely would leave the previous (often
                        /// much larger) hard limit in place, defeating the whole point of dynamic
                        /// adjustment: that ClickHouse should *shrink* its budget when free memory
                        /// is gone, so co-located processes are not killed.
                        ///
                        /// Clamp instead of skipping: keep a small safety margin above `used` so
                        /// queries can still allocate between ticks, but still apply the shrink so
                        /// subsequent allocations are throttled.
                        static constexpr Int64 safety_margin = 64ll * 1024 * 1024;
                        new_hard_limit = std::max(new_hard_limit, used + safety_margin);

                        /// Never exceed the configured `max_server_memory_usage`. The dynamic
                        /// adjustment may only shrink the budget further, not raise it above the
                        /// explicit user setting. This must come *after* the `used + margin` floor:
                        /// if we are already over `ceiling`, we cannot shrink to below `used`
                        /// without strangling our own queries, but we still must not exceed `ceiling`.
                        if (ceiling > 0)
                            new_hard_limit = std::min(new_hard_limit, ceiling);

                        Int64 current_hard_limit = total_memory_tracker.getHardLimit();
                        if (new_hard_limit != current_hard_limit)
                        {
                            /// Defeat the reload race: take the apply mutex and re-check the
                            /// generation under it. If a concurrent `setDynamicHardLimitSettings`
                            /// already installed its own value, the generation will have changed
                            /// and we skip the write so the reload's value persists. The mutex
                            /// also rules out a TOCTOU window between the check and the apply.
                            std::lock_guard apply_lock(dynamic_hard_limit_apply_mutex);
                            if (settings_generation.load(std::memory_order_acquire) == gen_before)
                            {
                                LOG_TRACE(
                                    log,
                                    "Adjusting total memory hard limit from {} to {} (resident: {}, available: {}, ceiling: {}, ratio: {})",
                                    formatReadableSizeWithBinarySuffix(current_hard_limit),
                                    formatReadableSizeWithBinarySuffix(new_hard_limit),
                                    formatReadableSizeWithBinarySuffix(used),
                                    formatReadableSizeWithBinarySuffix(available),
                                    formatReadableSizeWithBinarySuffix(ceiling),
                                    ratio);
                                total_memory_tracker.setHardLimit(new_hard_limit);
                            }
                        }
                    }
                }
            }

            ProfileEvents::increment(ProfileEvents::MemoryWorkerRun);
            ProfileEvents::increment(ProfileEvents::MemoryWorkerRunElapsedMicroseconds, total_watch.elapsedMicroseconds());
            first_run = false;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to update resident memory");
        }
    }
}

#if USE_JEMALLOC
void MemoryWorker::setDirtyDecayForAllArenas(size_t decay_ms)
{
    try
    {
        /// First, set the default for any NEW arenas that get created
        Jemalloc::setValue("arenas.dirty_decay_ms", decay_ms);

        /// Now update all EXISTING arenas
        /// Query how many arenas currently exist
        unsigned narenas = 0;
        if (!Jemalloc::tryGetValue("arenas.narenas", narenas))
        {
            LOG_TRACE(log, "jemalloc mallctl arenas.narenas unavailable; skipping per-arena dirty_decay_ms update");
            return;
        }

        /// Iterate through each arena and set its dirty_decay_ms
        for (unsigned i = 0; i < narenas; ++i)
        {
            std::string arena_path = fmt::format("arena.{}.dirty_decay_ms", i);

            try
            {
                Jemalloc::setValue(arena_path.c_str(), decay_ms);
            }
            catch (...) // Ok: some arenas might not exist or be accessible, skip them
            {
                /// Some arenas might not exist or be accessible, skip them
                LOG_TRACE(log, "Failed to set dirty_decay_ms for arena {}", i);
            }
        }
    }
    catch (...) // Ok: jemalloc arena config is best-effort
    {
        tryLogCurrentException(log, "Failed to set dirty_decay_ms");
    }
}

void MemoryWorker::purgeDirtyPagesThread()
{
    /// Instead of having completely separate logic for purging dirty pages,
    /// we rely on the main thread to notify us when we need to purge dirty pages.
    /// We do it to avoid reading RSS value in both threads. Even though they are fairly
    /// fast, they are still not free.
    /// So we keep the work of reading current RSS in one thread which allows us to keep the low period time for it.
    DB::setThreadName(ThreadName::MEMORY_WORKER);

    std::unique_lock purge_dirty_pages_lock(purge_dirty_pages_mutex);

    uint64_t default_dirty_decay_ms = dirty_decay_ms_mib.getValue();
    LOG_INFO(log, "Default dirty pages decay period: {}ms", default_dirty_decay_ms);

    /// On low-memory systems (< 4 GiB), disable jemalloc dirty page retention
    /// (dirty_decay_ms=0) to prevent RSS inflation.
    {
        size_t available_memory = getMemoryAmount();
        if (available_memory > 0 && available_memory < (4ul << 30))
        {
            LOG_INFO(log, "Low memory system detected ({}). Setting dirty_decay_ms=0",
                formatReadableSizeWithBinarySuffix(available_memory));
            setDirtyDecayForAllArenas(0);
            default_dirty_decay_ms = 0;
        }
    }
    while (true)
    {
        try
        {
            /// We add timeout of 1 second to protect against rare race condition where
            /// signal could be missed leading to this thread being suck forever.
            /// We cannot use mutex in RSS update thread because we want to keep them independent,
            /// i.e. purging dirty pages should not block RSS update.
            purge_dirty_pages_cv.wait_for(
                purge_dirty_pages_lock,
                std::chrono::seconds(1),
                [&]
                {
                    auto state = decay_state.load(std::memory_order_relaxed);
                    return shutdown || purge_dirty_pages.load(std::memory_order_relaxed)
                        || state == MemoryWorker::DecayState::DisableRequested || state == MemoryWorker::DecayState::EnableRequested;
                });

            if (shutdown)
                return;

            /// Handle decay state transitions
            auto current_state = decay_state.load(std::memory_order_relaxed);
            if (current_state == MemoryWorker::DecayState::DisableRequested)
            {
                LOG_INFO(
                    log,
                    "Setting jemalloc's dirty pages decay period to 0ms (disabling automatic decay) because of high memory usage for a "
                    "longer "
                    "period of time (> {}ms). This should provide server with more memory but it could negatively impact the performance",
                    decay_adjustment_period_ms.count());
                setDirtyDecayForAllArenas(0);
                decay_state.store(MemoryWorker::DecayState::Disabled, std::memory_order_relaxed);
            }
            else if (current_state == MemoryWorker::DecayState::EnableRequested)
            {
                LOG_INFO(
                    log,
                    "Setting jemalloc's dirty pages decay period to {}ms (re-enabling automatic decay). Server has been operating with "
                    "normal "
                    "memory usage for at least {}ms",
                    default_dirty_decay_ms,
                    decay_adjustment_period_ms.count());
                setDirtyDecayForAllArenas(default_dirty_decay_ms);
                decay_state.store(MemoryWorker::DecayState::Enabled, std::memory_order_relaxed);
            }

            bool is_purge_enabled = true;
            if (!purge_dirty_pages.compare_exchange_strong(is_purge_enabled, false, std::memory_order_relaxed))
                continue;

            Stopwatch purge_watch;
            purge_mib.run();
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, purge_watch.elapsedMicroseconds());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to purge dirty pages");
        }
    }
}
#endif

}
