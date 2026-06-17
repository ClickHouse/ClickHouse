#pragma once

#include <Common/ThreadPool.h>

#include <chrono>
#include <mutex>

namespace DB
{

///  Periodically reads the the maximum memory available to the process (which can change due to cgroups settings).
///  You can specify a callback to react on changes. The callback typically reloads the configuration, i.e. Server
///  or Keeper configuration file. This reloads settings 'max_server_memory_usage' (Server) and 'max_memory_usage_soft_limit'
///  (Keeper) from which various other internal limits are calculated, including the soft and hard limits for (1.).
///  The goal of this is to provide elasticity when the container is scaled-up/scaled-down. The mechanism (polling
///  cgroups) is quite implicit, unfortunately there is currently no better way to communicate memory threshold changes
///  to the database.
#if defined(OS_LINUX)
class CgroupsMemoryUsageObserver
{
public:
    using OnMemoryAmountAvailableChangedFn = std::function<void()>;

    explicit CgroupsMemoryUsageObserver(std::chrono::seconds wait_time_);
    ~CgroupsMemoryUsageObserver();

    void setOnMemoryAmountAvailableChangedFn(OnMemoryAmountAvailableChangedFn on_memory_amount_available_changed_);

    void startThread();

private:
    LoggerPtr log;

    const std::chrono::seconds wait_time;

    std::mutex limit_mutex;

    std::mutex memory_amount_available_changed_mutex;
    OnMemoryAmountAvailableChangedFn on_memory_amount_available_changed TSA_GUARDED_BY(memory_amount_available_changed_mutex);

    uint64_t last_available_memory_amount; /// how much memory can the process use

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
