#pragma once

#include <Common/ThreadPool.h>

#include <atomic>
#include <chrono>
#include <mutex>

namespace DB
{

/// Periodically reads the current memory usage from Linux cgroups.
/// You can specify soft or hard memory limits:
/// - When the soft memory limit is hit, drop jemalloc cache.
/// - When the hard memory limit is hit, update MemoryTracking metric to throw memory exceptions faster.
#if defined(OS_LINUX)
class CgroupsMemoryUsageObserver
{
public:
    enum class CgroupsVersion
    {
        V1,
        V2

    };
    explicit CgroupsMemoryUsageObserver(std::chrono::seconds wait_time_);
    ~CgroupsMemoryUsageObserver();

    void setLimits(uint64_t hard_limit_, uint64_t soft_limit_);

    size_t getHardLimit() const { return hard_limit; }
    size_t getSoftLimit() const { return soft_limit; }

    uint64_t readMemoryUsage() const;

private:
    LoggerPtr log;

    std::atomic<size_t> hard_limit = 0;
    std::atomic<size_t> soft_limit = 0;

    const std::chrono::seconds wait_time;

    using CallbackFn = std::function<void(bool)>;
    CallbackFn on_hard_limit;
    CallbackFn on_soft_limit;

    uint64_t last_usage = 0;

    /// Represents the cgroup virtual file that shows the memory consumption of the process's cgroup.
    struct File
    {
    public:
        explicit File(LoggerPtr log_);
        ~File();
        uint64_t readMemoryUsage() const;
    private:
        LoggerPtr log;
        mutable std::mutex mutex;
        int fd TSA_GUARDED_BY(mutex) = -1;
        CgroupsVersion version;
        std::string file_name;
    };

    File file;

    void startThread();
    void stopThread();

    void runThread();
    void processMemoryUsage(uint64_t usage);

    std::mutex thread_mutex;
    std::condition_variable cond;
    ThreadFromGlobalPool thread;
    bool quit = false;
};

#else
class CgroupsMemoryUsageObserver
{
public:
    explicit CgroupsMemoryUsageObserver(std::chrono::seconds) {}

    void setLimits(uint64_t, uint64_t) {}
    size_t readMemoryUsage() { return 0; }
    size_t getHardLimit() { return 0; }
    size_t getSoftLimit() { return 0; }
};
#endif

}
