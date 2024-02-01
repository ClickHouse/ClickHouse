#include <Common/CgroupsMemoryUsageObserver.h>

#if defined(OS_LINUX)

#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <base/sleep.h>

#include <filesystem>
#include <optional>

#include "config.h"
#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_DATA;
}

CgroupsMemoryUsageObserver::CgroupsMemoryUsageObserver(std::chrono::seconds wait_time_)
    : log(getLogger("CgroupsMemoryUsageObserver"))
    , wait_time(wait_time_)
    , file(log)
{
    LOG_INFO(log, "Initialized cgroups memory limit observer, wait time is {} sec", wait_time.count());
}

CgroupsMemoryUsageObserver::~CgroupsMemoryUsageObserver()
{
    stopThread();
}

void CgroupsMemoryUsageObserver::setLimits(uint64_t hard_limit_, uint64_t soft_limit_)
{
    if (hard_limit_ == hard_limit && soft_limit_ == soft_limit)
        return;

    stopThread();

    hard_limit = hard_limit_;
    soft_limit = soft_limit_;

    on_hard_limit = [this, hard_limit_](bool up)
    {
        if (up)
        {
            LOG_WARNING(log, "Exceeded hard memory limit ({})", ReadableSize(hard_limit_));

            /// Update current usage in memory tracker. Also reset free_memory_in_allocator_arenas to zero though we don't know if they are
            /// really zero. Trying to avoid OOM ...
            MemoryTracker::setRSS(hard_limit_, 0);
        }
        else
        {
            LOG_INFO(log, "Dropped below hard memory limit ({})", ReadableSize(hard_limit_));
        }
    };

    on_soft_limit = [this, soft_limit_](bool up)
    {
        if (up)
        {
            LOG_WARNING(log, "Exceeded sort memory limit ({})", ReadableSize(soft_limit_));

#if USE_JEMALLOC
            LOG_INFO(log, "Purging jemalloc arenas");
            mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
#endif
            /// Reset current usage in memory tracker. Expect zero for free_memory_in_allocator_arenas as we just purged them.
            uint64_t current_usage = readMemoryUsage();
            MemoryTracker::setRSS(current_usage, 0);

            LOG_INFO(log, "Purged jemalloc arenas. Current memory usage is {}", ReadableSize(current_usage));
        }
        else
        {
            LOG_INFO(log, "Dropped below soft memory limit ({})", ReadableSize(soft_limit_));
        }
    };

    startThread();

    LOG_INFO(log, "Set new limits, soft limit: {}, hard limit: {}", ReadableSize(soft_limit_), ReadableSize(hard_limit_));
}

uint64_t CgroupsMemoryUsageObserver::readMemoryUsage() const
{
    return file.readMemoryUsage();
}

namespace
{

/// I think it is possible to mount the cgroups hierarchy somewhere else (e.g. when in containers).
/// /sys/fs/cgroup was still symlinked to the actual mount in the cases that I have seen.
const std::filesystem::path default_cgroups_mount = "/sys/fs/cgroup";

/// Caveats:
/// - All of the logic in this file assumes that the current process is the only process in the
///   containing cgroup (or more precisely: the only process with significant memory consumption).
///   If this is not the case, then other processe's memory consumption may affect the internal
///   memory tracker ...
/// - Cgroups v1 and v2 allow nested cgroup hierarchies. As v1 is deprecated for over half a
///   decade and will go away at some point, hierarchical detection is only implemented for v2.
/// - I did not test what happens if a host has v1 and v2 simultaneously enabled. I believe such
///   systems existed only for a short transition period.

std::optional<std::string> getCgroupsV2FileName()
{
    /// This file exists iff the host has cgroups v2 enabled.
    auto controllers_file_path = default_cgroups_mount / "cgroup.controllers";
    if (!std::filesystem::exists(controllers_file_path))
        return {};

    /// Make sure that the memory controller is enabled.
    /// - cgroup.controllers defines which controllers *can* be enabled.
    /// - cgroup.subtree_control defines which controllers *are* enabled.
    /// (see https://docs.kernel.org/admin-guide/cgroup-v2.html)
    /// Caveat: child cgroups may disable controllers but such a situation should be very rare.
    /// Therefore, only check the top-level cgroup for simplicity.
    ReadBufferFromFile subtree_control_file(default_cgroups_mount / "cgroup.subtree_control");
    std::string subtree_control;
    readString(subtree_control, subtree_control_file);
    if (subtree_control.find("memory") == std::string::npos)
        return {};

    /// Identify the cgroup the process belongs to.
    /// All PIDs assigned to a cgroup are in /sys/fs/cgroups/{cgroup_name}/cgroup.procs
    /// A simpler way to get the membership is:
    ReadBufferFromFile cgroup_file("/proc/self/cgroup");
    std::string cgroup;
    readString(cgroup, cgroup_file);
    /// With cgroups v2, there will be a *single* line with prefix "0::/"
    const std::string v2_prefix = "0::/";
    if (!cgroup.starts_with(v2_prefix))
        return {};
    cgroup = cgroup.substr(v2_prefix.length());

    auto current_cgroup = cgroup.empty() ? default_cgroups_mount : (default_cgroups_mount / cgroup);

    /// Return the bottom-most nested current memory file. If there is no such file at the current
    /// level, try again at the parent level as memory settings are inherited.
    while (current_cgroup != default_cgroups_mount.parent_path())
    {
        auto path = current_cgroup / "memory.current";
        if (std::filesystem::exists(path))
            return {path};
        current_cgroup = current_cgroup.parent_path();
    }
    return {};
}

std::optional<std::string> getCgroupsV1FileName()
{
    auto path = default_cgroups_mount / "memory/memory.stat";
    if (!std::filesystem::exists(path))
        return {};
    return {path};
}

std::pair<std::string, CgroupsMemoryUsageObserver::CgroupsVersion> getCgroupsFileName()
{
    auto v2_file_name = getCgroupsV2FileName();
    if (v2_file_name.has_value())
        return {*v2_file_name, CgroupsMemoryUsageObserver::CgroupsVersion::V2};

    auto v1_file_name = getCgroupsV1FileName();
    if (v1_file_name.has_value())
        return {*v1_file_name, CgroupsMemoryUsageObserver::CgroupsVersion::V1};

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot find cgroups v1 or v2 current memory file");
}

}

CgroupsMemoryUsageObserver::File::File(LoggerPtr log_)
    : log(log_)
{
    std::tie(file_name, version) = getCgroupsFileName();

    LOG_INFO(log, "Will read the current memory usage from '{}' (cgroups version: {})", file_name, (version == CgroupsVersion::V1) ? "v1" : "v2");

    fd = ::open(file_name.data(), O_RDONLY);
    if (fd == -1)
        ErrnoException::throwFromPath(
            (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE,
            file_name, "Cannot open file '{}'", file_name);
}

CgroupsMemoryUsageObserver::File::~File()
{
    assert(fd != -1);
    if (::close(fd) != 0)
    {
        try
        {
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_CLOSE_FILE,
                file_name, "Cannot close file '{}'", file_name);
        }
        catch (const ErrnoException &)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

uint64_t CgroupsMemoryUsageObserver::File::readMemoryUsage() const
{
    /// File read is probably not read is thread-safe, just to be sure
    std::lock_guard lock(mutex);

    ReadBufferFromFileDescriptor buf(fd);
    buf.rewind();

    uint64_t mem_usage;

    switch (version)
    {
        case CgroupsVersion::V1:
        {
            /// Format is
            ///   kernel 5
            ///   rss 15
            ///   [...]
            std::string key;
            while (!buf.eof())
            {
                readStringUntilWhitespace(key, buf);
                if (key != "rss")
                    continue;
                assertChar(' ', buf);
                readIntText(mem_usage, buf);
                assertChar('\n', buf);
                break;
            }
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot find 'rss' in '{}'", file_name);
        }
        case CgroupsVersion::V2:
        {
            readIntText(mem_usage, buf);
            break;
        }
    }

    LOG_TRACE(log, "Read current memory usage {} from cgroups", ReadableSize(mem_usage));

    return mem_usage;
}

void CgroupsMemoryUsageObserver::startThread()
{
    if (!thread.joinable())
    {
        thread = ThreadFromGlobalPool(&CgroupsMemoryUsageObserver::runThread, this);
        LOG_INFO(log, "Started cgroup current memory usage observer thread");
    }
}

void CgroupsMemoryUsageObserver::stopThread()
{
    {
        std::lock_guard lock(thread_mutex);
        if (!thread.joinable())
            return;
        quit = true;
    }

    cond.notify_one();
    thread.join();

    LOG_INFO(log, "Stopped cgroup current memory usage observer thread");
}

void CgroupsMemoryUsageObserver::runThread()
{
    setThreadName("CgrpMemUsgObsr");

    std::unique_lock lock(thread_mutex);
    while (true)
    {
        if (cond.wait_for(lock, wait_time, [this] { return quit; }))
            break;

        try
        {
            uint64_t memory_usage = file.readMemoryUsage();
            processMemoryUsage(memory_usage);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

void CgroupsMemoryUsageObserver::processMemoryUsage(uint64_t current_usage)
{
    if (current_usage > hard_limit)
    {
        if (last_usage <= hard_limit)
            on_hard_limit(true);
    }
    else
    {
        if (last_usage > hard_limit)
            on_hard_limit(false);
    }

    if (current_usage > soft_limit)
    {
        if (last_usage <= soft_limit)
            on_soft_limit(true);
    }
    else
    {
        if (last_usage > soft_limit)
            on_soft_limit(false);
    }

    last_usage = current_usage;
}

}

#endif
