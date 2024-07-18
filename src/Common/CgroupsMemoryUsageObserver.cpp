#include <Common/CgroupsMemoryUsageObserver.h>

#if defined(OS_LINUX)

#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <base/cgroupsv2.h>
#include <base/getMemoryAmount.h>
#include <base/sleep.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

using namespace DB;
namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int INCORRECT_DATA;
}

}

namespace
{

/// Format is
///   kernel 5
///   rss 15
///   [...]
uint64_t readMetricFromStatFile(ReadBufferFromFile & buf, const std::string & key)
{
    while (!buf.eof())
    {
        std::string current_key;
        readStringUntilWhitespace(current_key, buf);
        if (current_key != key)
        {
            std::string dummy;
            readStringUntilNewlineInto(dummy, buf);
            buf.ignore();
            continue;
        }

        assertChar(' ', buf);
        uint64_t value = 0;
        readIntText(value, buf);
        return value;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot find '{}' in '{}'", key, buf.getFileName());
}

struct CgroupsV1Reader : ICgroupsReader
{
    explicit CgroupsV1Reader(const fs::path & stat_file_dir) : buf(stat_file_dir / "memory.stat") { }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        buf.rewind();
        return readMetricFromStatFile(buf, "rss");
    }

private:
    std::mutex mutex;
    ReadBufferFromFile buf TSA_GUARDED_BY(mutex);
};

struct CgroupsV2Reader : ICgroupsReader
{
    explicit CgroupsV2Reader(const fs::path & stat_file_dir)
        : current_buf(stat_file_dir / "memory.current"), stat_buf(stat_file_dir / "memory.stat")
    {
    }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        current_buf.rewind();
        stat_buf.rewind();

        int64_t mem_usage = 0;
        /// memory.current contains a single number
        /// the reason why we subtract it described here: https://github.com/ClickHouse/ClickHouse/issues/64652#issuecomment-2149630667
        readIntText(mem_usage, current_buf);
        mem_usage -= readMetricFromStatFile(stat_buf, "inactive_file");
        chassert(mem_usage >= 0, "Negative memory usage");
        return mem_usage;
    }

private:
    std::mutex mutex;
    ReadBufferFromFile current_buf TSA_GUARDED_BY(mutex);
    ReadBufferFromFile stat_buf TSA_GUARDED_BY(mutex);
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

std::optional<std::string> getCgroupsV2Path()
{
    if (!cgroupsV2Enabled())
        return {};

    if (!cgroupsV2MemoryControllerEnabled())
        return {};

    fs::path current_cgroup = cgroupV2PathOfProcess();
    if (current_cgroup.empty())
        return {};

    /// Return the bottom-most nested current memory file. If there is no such file at the current
    /// level, try again at the parent level as memory settings are inherited.
    while (current_cgroup != default_cgroups_mount.parent_path())
    {
        const auto current_path = current_cgroup / "memory.current";
        const auto stat_path = current_cgroup / "memory.stat";
        if (fs::exists(current_path) && fs::exists(stat_path))
            return {current_cgroup};
        current_cgroup = current_cgroup.parent_path();
    }
    return {};
}

std::optional<std::string> getCgroupsV1Path()
{
    auto path = default_cgroups_mount / "memory/memory.stat";
    if (!fs::exists(path))
        return {};
    return {default_cgroups_mount / "memory"};
}

enum class CgroupsVersion : uint8_t
{
    V1,
    V2
};

std::pair<std::string, CgroupsVersion> getCgroupsPath()
{
    auto v2_path = getCgroupsV2Path();
    if (v2_path.has_value())
        return {*v2_path, CgroupsVersion::V2};

    auto v1_path = getCgroupsV1Path();
    if (v1_path.has_value())
        return {*v1_path, CgroupsVersion::V1};

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot find cgroups v1 or v2 current memory file");
}

}

namespace DB
{

std::shared_ptr<ICgroupsReader> createCgroupsReader()
{
    const auto [cgroup_path, version] = getCgroupsPath();
    LOG_INFO(
        getLogger("CgroupsReader"),
        "Will create cgroup reader from '{}' (cgroups version: {})",
        cgroup_path,
        (version == CgroupsVersion::V1) ? "v1" : "v2");

    if (version == CgroupsVersion::V2)
        return std::make_shared<CgroupsV2Reader>(cgroup_path);
    else
    {
        chassert(version == CgroupsVersion::V1);
        return std::make_shared<CgroupsV1Reader>(cgroup_path);
    }

}

CgroupsMemoryUsageObserver::CgroupsMemoryUsageObserver(std::chrono::seconds wait_time_)
    : log(getLogger("CgroupsMemoryUsageObserver")), wait_time(wait_time_), cgroups_reader(createCgroupsReader())
{}

CgroupsMemoryUsageObserver::~CgroupsMemoryUsageObserver()
{
    stopThread();
}

void CgroupsMemoryUsageObserver::setOnMemoryAmountAvailableChangedFn(OnMemoryAmountAvailableChangedFn on_memory_amount_available_changed_)
{
    std::lock_guard<std::mutex> memory_amount_available_changed_lock(memory_amount_available_changed_mutex);
    on_memory_amount_available_changed = on_memory_amount_available_changed_;
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

    last_available_memory_amount = getMemoryAmount();
    LOG_INFO(log, "Memory amount initially available to the process is {}", ReadableSize(last_available_memory_amount));

    std::unique_lock lock(thread_mutex);
    while (true)
    {
        if (cond.wait_for(lock, wait_time, [this] { return quit; }))
            break;

        try
        {
            uint64_t available_memory_amount = getMemoryAmount();
            if (available_memory_amount != last_available_memory_amount)
            {
                LOG_INFO(log, "Memory amount available to the process changed from {} to {}", ReadableSize(last_available_memory_amount), ReadableSize(available_memory_amount));
                last_available_memory_amount = available_memory_amount;
                std::lock_guard<std::mutex> memory_amount_available_changed_lock(memory_amount_available_changed_mutex);
                on_memory_amount_available_changed();
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

}

#endif
