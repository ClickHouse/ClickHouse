#include <Common/MemoryWorker.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <base/cgroupsv2.h>
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

using Metrics = std::map<std::string, uint64_t>;

/// Format is
///   kernel 5
///   rss 15
///   [...]
Metrics readAllMetricsFromStatFile(ReadBufferFromFile & buf)
{
    Metrics metrics;
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

uint64_t readMetricsFromStatFile(ReadBufferFromFile & buf, std::initializer_list<std::string_view> keys, std::initializer_list<std::string_view> optional_keys, bool * warnings_printed)
{
    uint64_t sum = 0;
    uint64_t found_mask = 0;
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
            buf.tryIgnore(1); /// skip EOL (if not EOF)
            continue;
        }

        if (print_warnings && (found_mask & (1l << (it - keys.begin()))))
        {
            *warnings_printed = true;
            LOG_ERROR(getLogger("CgroupsReader"), "Duplicate key '{}' in '{}'", current_key, buf.getFileName());
        }
        found_mask |= 1ll << (it - keys.begin());

        assertChar(' ', buf);
        uint64_t value = 0;
        readIntText(value, buf);
        sum += value;
        buf.tryIgnore(1); /// skip EOL (if not EOF)
    }

    /// Did we see all keys?
    for (const auto * it = keys.begin(); it != keys.end(); ++it)
    {
        if (print_warnings
                && !(found_mask & (1l << (it - keys.begin())))
                && std::find(optional_keys.begin(), optional_keys.end(), *it) == optional_keys.end())
        {
            *warnings_printed = true;
            LOG_ERROR(getLogger("CgroupsReader"), "Cannot find '{}' in '{}'", *it, buf.getFileName());
        }
    }
    return sum;
}

struct CgroupsV1Reader : ICgroupsReader
{
    explicit CgroupsV1Reader(const fs::path & stat_file_dir) : buf(stat_file_dir / "memory.stat") { }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        buf.rewind();
        return readMetricsFromStatFile(buf, {"rss"}, {}, &warnings_printed);
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
    bool warnings_printed TSA_GUARDED_BY(mutex) = false;
};

struct CgroupsV2Reader : ICgroupsReader
{
    explicit CgroupsV2Reader(const fs::path & stat_file_dir) : stat_buf(stat_file_dir / "memory.stat") { }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        stat_buf.rewind();
        return readMetricsFromStatFile(stat_buf, {"anon", "sock", "kernel"}, {"kernel"}, &warnings_printed);
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
    , page_cache(page_cache_)
{
#if USE_JEMALLOC
    page_size = pagesize_mib.getValue();
#endif

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
        unsigned narenas = Jemalloc::getValue<unsigned>("arenas.narenas");

        /// Iterate through each arena and set its dirty_decay_ms
        for (unsigned i = 0; i < narenas; ++i)
        {
            std::string arena_path = fmt::format("arena.{}.dirty_decay_ms", i);

            try
            {
                Jemalloc::setValue(arena_path.c_str(), decay_ms);
            }
            catch (...)
            {
                /// Some arenas might not exist or be accessible, skip them
                LOG_TRACE(log, "Failed to set dirty_decay_ms for arena {}", i);
            }
        }
    }
    catch (...)
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
