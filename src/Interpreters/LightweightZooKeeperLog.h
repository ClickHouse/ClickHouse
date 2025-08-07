#pragma once

#include <filesystem>
#include <Interpreters/PeriodicLog.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ErrorCounter.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeEnum.h>

namespace DB
{

struct LightweightZooKeeperLogElement
{
    /// Identifying a group.
    time_t event_time;
    String parent_path;
    Coordination::OpNum operation;
    
    /// Group statistics.
    UInt32 count;
    Coordination::ErrorCounter errors;
    UInt64 total_latency_ms;

    static std::string name() { return "LightweightZooKeeperLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class LightweightZooKeeperLog : public PeriodicLog<LightweightZooKeeperLogElement>
{
    using PeriodicLog<LightweightZooKeeperLogElement>::PeriodicLog;

protected:
    void stepFunction(TimePoint current_time) override
    {
        std::lock_guard lock(stats_mutex);
        for (auto & [entry_key, entry_stats] : stats)
        {
            LightweightZooKeeperLogElement element{
                .event_time = std::chrono::system_clock::to_time_t(current_time),
                .parent_path = entry_key.parent_path,
                .operation = entry_key.operation,
                .count = entry_stats.count,
                .errors = std::move(entry_stats.errors),
                .total_latency_ms = entry_stats.total_latency_ms,
            };
            add(std::move(element));
        }
        stats.clear();
    }

public:
    void observe(Coordination::OpNum operation, const std::filesystem::path & path, UInt32 latency_ms, Coordination::Error error)
    {
        std::lock_guard lock(stats_mutex);
        stats[EntryKey{.operation = operation, .parent_path = path.parent_path()}].observe(latency_ms, error);
    }

private:
    struct EntryKey
    {
        Coordination::OpNum operation;
        String parent_path;

        bool operator==(const EntryKey & other) const
        {
            return operation == other.operation && parent_path == other.parent_path;
        }
    };
    struct EntryKeyHash
    {
        size_t operator()(const EntryKey & entry_key) const
        {
            SipHash hash;
            hash.update(entry_key.operation);
            hash.update(entry_key.parent_path);
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

    mutable std::mutex stats_mutex;
    std::unordered_map<EntryKey, EntryStats, EntryKeyHash> stats TSA_GUARDED_BY(stats_mutex);
};

}
