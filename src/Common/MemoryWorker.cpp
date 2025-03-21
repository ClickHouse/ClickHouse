#include <Common/MemoryWorker.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <base/cgroupsv2.h>
#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

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
    extern const int LOGICAL_ERROR;
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

uint64_t readMetricsFromStatFile(ReadBufferFromFile & buf, std::initializer_list<std::string_view> keys, bool * warnings_printed)
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
            buf.ignore();
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
    }
    if (found_mask != (1l << keys.size()) - 1)
    {
        for (const auto * it = keys.begin(); it != keys.end(); ++it)
        {
            if (print_warnings && (!(found_mask & (1l << (it - keys.begin())))))
            {
                *warnings_printed = true;
                LOG_ERROR(getLogger("CgroupsReader"), "Cannot find '{}' in '{}'", *it, buf.getFileName());
            }
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
        return readMetricsFromStatFile(buf, {"rss"}, &warnings_printed);
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
        return readMetricsFromStatFile(stat_buf, {"anon", "sock", "kernel"}, &warnings_printed);
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

std::pair<std::string, ICgroupsReader::CgroupsVersion> getCgroupsPath()
{
    auto v2_path = getCgroupsV2PathContainingFile("memory.current");
    if (v2_path.has_value())
        return {*v2_path, ICgroupsReader::CgroupsVersion::V2};

    auto v1_path = getCgroupsV1Path();
    if (v1_path.has_value())
        return {*v1_path, ICgroupsReader::CgroupsVersion::V1};

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot find cgroups v1 or v2 current memory file");
}

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
MemoryWorker::MemoryWorker(uint64_t period_ms_, bool correct_tracker_, bool use_cgroup, std::shared_ptr<PageCache> page_cache_)
    : log(getLogger("MemoryWorker"))
    , period_ms(period_ms_)
    , correct_tracker(correct_tracker_)
    , page_cache(page_cache_)
{
    if (use_cgroup)
    {
#if defined(OS_LINUX)
        try
        {
            static constexpr uint64_t cgroups_memory_usage_tick_ms{50};

            const auto [cgroup_path, version] = getCgroupsPath();
            LOG_INFO(
                getLogger("CgroupsReader"),
                "Will create cgroup reader from '{}' (cgroups version: {})",
                cgroup_path,
                (version == ICgroupsReader::CgroupsVersion::V1) ? "v1" : "v2");

            cgroups_reader = ICgroupsReader::createCgroupsReader(version, cgroup_path);
            source = MemoryUsageSource::Cgroups;
            if (period_ms == 0)
                period_ms = cgroups_memory_usage_tick_ms;

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
    if (period_ms == 0)
        period_ms = jemalloc_memory_usage_tick_ms;
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

    LOG_INFO(
        getLogger("MemoryWorker"),
        "Starting background memory thread with period of {}ms, using {} as source",
        period_ms,
        sourceToString(source));
    background_thread = ThreadFromGlobalPool([this] { backgroundThread(); });
}

MemoryWorker::~MemoryWorker()
{
    {
        std::unique_lock lock(mutex);
        shutdown = true;
    }
    cv.notify_all();

    if (background_thread.joinable())
        background_thread.join();
}

uint64_t MemoryWorker::getMemoryUsage()
{
    switch (source)
    {
        case MemoryUsageSource::Cgroups:
            return cgroups_reader != nullptr ? cgroups_reader->readMemoryUsage() : 0;
        case MemoryUsageSource::Jemalloc:
#if USE_JEMALLOC
            return resident_mib.getValue();
#else
            return 0;
#endif
        case MemoryUsageSource::None:
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Trying to fetch memory usage while no memory source can be used");
    }
}

void MemoryWorker::backgroundThread()
{
    std::chrono::milliseconds chrono_period_ms{period_ms};
    [[maybe_unused]] bool first_run = true;
    std::unique_lock lock(mutex);
    while (true)
    {
        cv.wait_for(lock, chrono_period_ms, [this] { return shutdown; });
        if (shutdown)
            return;

        Stopwatch total_watch;

#if USE_JEMALLOC
        if (source == MemoryUsageSource::Jemalloc)
            epoch_mib.setValue(0);
#endif

        Int64 resident = getMemoryUsage();
        MemoryTracker::updateRSS(resident);

        if (page_cache)
            page_cache->autoResize(resident, total_memory_tracker.getHardLimit());

#if USE_JEMALLOC
        if (resident > total_memory_tracker.getHardLimit())
        {
            Stopwatch purge_watch;
            purge_mib.run();
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, purge_watch.elapsedMicroseconds());
        }

        /// update MemoryTracker with `allocated` information from jemalloc when:
        ///  - it's a first run of MemoryWorker (MemoryTracker could've missed some allocation before its initialization)
        ///  - MemoryTracker stores a negative value
        ///  - `correct_tracker` is set to true
        if (unlikely(first_run || total_memory_tracker.get() < 0))
        {
            if (source != MemoryUsageSource::Jemalloc)
                epoch_mib.setValue(0);

            MemoryTracker::updateAllocated(allocated_mib.getValue(), /*log_change=*/true);
        }
        else if (correct_tracker)
        {
            if (source != MemoryUsageSource::Jemalloc)
                epoch_mib.setValue(0);

            MemoryTracker::updateAllocated(allocated_mib.getValue(), /*log_change=*/false);
        }
#else
        /// we don't update in the first run if we don't have jemalloc
        /// because we can only use resident memory information
        /// resident memory can be much larger than the actual allocated memory
        /// so we rather ignore the potential difference caused by allocated memory
        /// before MemoryTracker initialization
        if (unlikely(total_memory_tracker.get() < 0) || correct_tracker)
            MemoryTracker::updateAllocated(resident, /*log_change=*/false);

#endif

        ProfileEvents::increment(ProfileEvents::MemoryWorkerRun);
        ProfileEvents::increment(ProfileEvents::MemoryWorkerRunElapsedMicroseconds, total_watch.elapsedMicroseconds());
        first_run = false;
    }
}

}
