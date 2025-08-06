#pragma once

#include <Poco/Event.h>
#include <Core/BackgroundSchedulePool.h>
#include "Common/SipHash.h"
#include "Common/ZooKeeper/IKeeper.h"
#include <Common/logger_useful.h>
#include <base/types.h>
#include <atomic>
#include <mutex>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ErrorCounter.h>
#include "Interpreters/LightweightZooKeeperLog.h"
#include "base/defines.h"

namespace DB
{

class LightweightZooKeeperLoggerThread
{
public:
    struct EntryKey
    {
        Coordination::OpNum operation;
        String path_prefix;

        bool operator==(const EntryKey & other) const
        {
            return operation == other.operation && path_prefix == other.path_prefix;
        }
    };

    struct EntryKeyHash
    {
        size_t operator()(const EntryKey & entry_key) const
        {
            SipHash hash;
            hash.update(entry_key.operation);
            hash.update(entry_key.path_prefix);
            return hash.get64();
        }
    };

    struct EntryStats
    {
        UInt32 count = 0;
        UInt64 total_latency_ms = 0;
        Coordination::ErrorCounter errors;

        void observe(UInt32 latency_ms, Coordination::Error error)
        {
            ++count;
            total_latency_ms += latency_ms;
            errors.increment(error);
        }
    };

    explicit LightweightZooKeeperLoggerThread(UInt64 flush_period_ms_, UInt64 max_entries_, Int8 path_prefix_depth_, BackgroundSchedulePool & pool, std::shared_ptr<LightweightZooKeeperLog> log_)
    : log_name("LightweightZooKeeperLoggerThread")
    , flush_period_ms(flush_period_ms_)
    , max_entries(max_entries_)
    , path_prefix_depth(path_prefix_depth_)
    , task(pool.createTask(log_name, [this]{ run(); }))
    , log(log_)
    , logger(getLogger(log_name))
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

    String stripPath(String path)
    {
        /// TODO(mstetsyuk): use path_prefix_depth to strip the path the right way
        return "";
    }

    void observe(Coordination::OpNum operation, String path, UInt32 latency_ms, Coordination::Error error)
    {
        size_t stats_entries;
        {
            std::lock_guard lock(stats_mutex);
            stats[EntryKey{.operation = operation, .path_prefix = stripPath(std::move(path_prefix))}].observe(latency_ms, error);
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
            LOG_DEBUG(logger, "Flushing lightweight ZooKeeper log failed, will retry in {} ms", retry_period_ms);
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
    const Int8 path_prefix_depth;
    BackgroundSchedulePool::TaskHolder task;
    std::shared_ptr<LightweightZooKeeperLog> log;
    std::atomic<bool> need_stop{false};

    LoggerPtr logger;

    mutable std::mutex stats_mutex;
    std::unordered_map<EntryKey, EntryStats, EntryKeyHash> stats TSA_GUARDED_BY(stats_mutex);

    void runImpl()
    {
        std::lock_guard lock(stats_mutex);

        for (auto & [entry_key, entry_stats] : stats)
        {
            LightweightZooKeeperLogElement element{
                .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
                .path_prefix = entry_key.path_prefix,
                .operation = entry_key.operation,
                .count = entry_stats.count,
                .errors = std::move(entry_stats.errors),
                .total_latency_ms = entry_stats.total_latency_ms,
            };
            log->add(std::move(element));
        }

        stats.clear();
    }
};
}
