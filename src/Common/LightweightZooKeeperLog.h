#pragma once

#include <Poco/Event.h>
#include <Core/BackgroundSchedulePool.h>
#include "Common/SipHash.h"
#include <Common/logger_useful.h>
#include <base/types.h>
#include <atomic>
#include <mutex>
#include <Common/ZooKeeper/ZooKeeper.h>
#include "base/defines.h"

namespace DB
{

class LightweightZooKeeperLoggerThread
{
public:
    enum class Operation
    {
        Create,
        Remove,
        RemoveRecursive,
        Exists,
        Get,
        Set,
        List,
        Check,
        Sync,
        Reconfig,
        Multi,
    };

    struct EntryKey
    {
        Operation operation;
        String path;
    };

    struct EntryKeyHash
    {
        size_t operator()(const EntryKey & entry_key) const
        {
            SipHash hash;
            hash.update(entry_key.operation);
            hash.update(entry_key.path);
            return hash.get64();
        }
    };

    struct EntryStats
    {
        UInt32 count = 0;
        UInt64 total_latency_ms = 0;
        UInt32 max_latency_ms = 0;
        UInt32 errors = 0;

        void observe(UInt32 latency_ms, bool is_error)
        {
            ++count;
            total_latency_ms += 1;
            max_latency_ms = std::max(max_latency_ms, latency_ms);
            if (is_error)
            {
                ++errors;
            }
        }
    };

    explicit LightweightZooKeeperLoggerThread(UInt64 flush_period_ms_, UInt64 max_entries_, BackgroundSchedulePool & pool)
    : log_name("LightweightZooKeeperLoggerThread")
    , flush_period_ms(flush_period_ms_)
    , max_entries(max_entries_)
    , task(pool.createTask(log_name, [this]{ run(); }))
    , log(getLogger(log_name))
    {}

    void start()
    {
        task->activateAndSchedule();
    }

    void shutdown()
    {
        need_stop.store(true, std::memory_order_relaxed);
        task->deactivate();
    }

    void observe(Operation operation, String path, UInt32 latency_ms, bool is_error)
    {
        size_t stats_entries;
        {
            std::lock_guard lock(stats_mutex);
            stats[EntryKey{.operation = operation, .path = path}].observe(latency_ms, is_error);
            stats_entries = stats.size();
        }

        if (max_entries && stats_entries >= max_entries)
        {
            task->schedule();
        }
    }

    void run()
    {
        if (need_stop.load(std::memory_order_relaxed))
        {
            return;
        }
        
        UInt64 reschedule_period = flush_period_ms;

        try
        {
            runImpl();
        }
        catch (...)
        {
            LOG_DEBUG(log, "Flushing lightweight ZooKeeper log failed, will retry in {} ms", retry_period_ms);
            reschedule_period = retry_period_ms;
        }

        if (reschedule_period)
        {
            task->scheduleAfter(reschedule_period);
        }
    }


private:
    static constexpr UInt64 retry_period_ms = 1000;

    const String log_name;
    const UInt64 flush_period_ms;
    const UInt64 max_entries;
    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> need_stop{false};

    LoggerPtr log;

    mutable std::mutex stats_mutex;
    std::unordered_map<EntryKey, EntryStats, EntryKeyHash> stats TSA_GUARDED_BY(stats_mutex);

    void runImpl()
    {

    }
};
}
