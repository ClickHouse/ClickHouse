#pragma once

#include <Common/ThreadPool.h>

#include <chrono>
#include <mutex>

namespace DB
{

/// Does two things:
/// 1. Periodically reads the memory usage of the process from Linux cgroups.
///    You can specify soft or hard memory limits:
///    - When the soft memory limit is hit, drop jemalloc cache.
///    - When the hard memory limit is hit, update MemoryTracking metric to throw memory exceptions faster.
///    The goal of this is to avoid that the process hits the maximum allowed memory limit at which there is a good
///    chance that the Limux OOM killer terminates it. All of this is done is because internal memory tracking in
///    ClickHouse can unfortunately under-estimate the actually used memory.
/// 2. Periodically reads the the maximum memory available to the process (which can change due to cgroups settings).
///    You can specify a callback to react on changes. The callback typically reloads the configuration, i.e. Server
///    or Keeper configuration file. This reloads settings 'max_server_memory_usage' (Server) and 'max_memory_usage_soft_limit'
///    (Keeper) from which various other internal limits are calculated, including the soft and hard limits for (1.).
///    The goal of this is to provide elasticity when the container is scaled-up/scaled-down. The mechanism (polling
///    cgroups) is quite implicit, unfortunately there is currently no better way to communicate memory threshold changes
///    to the database.
#if defined(OS_LINUX)
class CgroupsMemoryUsageObserver
{
public:
    using OnMemoryLimitFn = std::function<void(bool)>;
    using OnMemoryAmountAvailableChangedFn = std::function<void()>;

    enum class CgroupsVersion : uint8_t
    {
        V1,
        V2
    };

    explicit CgroupsMemoryUsageObserver(std::chrono::seconds wait_time_);
    ~CgroupsMemoryUsageObserver();

    void setMemoryUsageLimits(uint64_t hard_limit_, uint64_t soft_limit_);
    void setOnMemoryAmountAvailableChangedFn(OnMemoryAmountAvailableChangedFn on_memory_amount_available_changed_);

    void startThread();

private:
    LoggerPtr log;

    const std::chrono::seconds wait_time;

    std::mutex limit_mutex;
    size_t hard_limit TSA_GUARDED_BY(limit_mutex) = 0;
    size_t soft_limit TSA_GUARDED_BY(limit_mutex) = 0;
    OnMemoryLimitFn on_hard_limit TSA_GUARDED_BY(limit_mutex);
    OnMemoryLimitFn on_soft_limit TSA_GUARDED_BY(limit_mutex);

    std::mutex memory_amount_available_changed_mutex;
    OnMemoryAmountAvailableChangedFn on_memory_amount_available_changed TSA_GUARDED_BY(memory_amount_available_changed_mutex);

    uint64_t last_memory_usage = 0;        /// how much memory does the process use
    uint64_t last_available_memory_amount; /// how much memory can the process use

    /// Represents the cgroup virtual file that shows the memory consumption of the process's cgroup.
    struct MemoryUsageFile
    {
    public:
        explicit MemoryUsageFile(LoggerPtr log_);
        ~MemoryUsageFile();
        uint64_t readMemoryUsage() const;
    private:
        LoggerPtr log;
        mutable std::mutex mutex;
        int fd TSA_GUARDED_BY(mutex) = -1;
        CgroupsVersion version;
        std::string file_name;
    };

    MemoryUsageFile memory_usage_file;

    void stopThread();

    void runThread();

    std::mutex thread_mutex;
    std::condition_variable cond;
    ThreadFromGlobalPool thread;
    bool quit = false;
};

#else
class CgroupsMemoryUsageObserver
{
    using OnMemoryAmountAvailableChangedFn = std::function<void()>;
public:
    explicit CgroupsMemoryUsageObserver(std::chrono::seconds) {}

    void setMemoryUsageLimits(uint64_t, uint64_t) {}
    void setOnMemoryAmountAvailableChangedFn(OnMemoryAmountAvailableChangedFn) {}
    void startThread() {}
};
#endif

}
